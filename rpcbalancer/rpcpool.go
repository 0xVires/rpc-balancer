package rpcbalancer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type node struct {
	URI     string
	Client  *fasthttp.Client
	Healthy bool
	Height  int64
	mu      sync.RWMutex
}

func NewNode(addr string) *node {
	metrics.SetHealth(addr, true)
	return &node{
		Client:  &fasthttp.Client{},
		Healthy: true,
		URI:     addr,
		Height:  0,
	}
}

// SetHealthy sets the health status of the node.
func (n *node) SetHealthy(healthy bool) {
	n.mu.Lock()
	changed := n.Healthy != healthy
	n.Healthy = healthy
	n.mu.Unlock()

	if changed {
		metrics.SetHealth(n.URI, healthy)
		log.WithFields(logrus.Fields{
			"node":    n.URI,
			"healthy": healthy,
		}).Info("Node health status changed")
	}
}

func (n *node) SetHeight(height int64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.Height = height
}

type Pool struct {
	Nodes           map[int]*node
	numNodes        int
	Fallback        *node
	SelectionMethod string
	RoundRobinChan  chan *node
	mu              sync.RWMutex
}

func NewPool(selectionMethod string) *Pool {
	return &Pool{
		Nodes:           make(map[int]*node),
		SelectionMethod: selectionMethod,
		RoundRobinChan:  make(chan *node, 1000),
		mu:              sync.RWMutex{},
	}
}

func (p *Pool) AddNode(n *node, id int) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.Nodes[id] = n
	p.RoundRobinChan <- n
	p.numNodes++
}

func (p *Pool) getHealthyNode() (*node, bool) {
	switch p.SelectionMethod {
	case "failover":
		return p.getNodeByFailoverOrder()
	case "roundrobin":
		return p.getNodeByRoundRobin()
	case "random":
		return p.getNodeByRandom()
	default:
		return p.getNodeByFailoverOrder()
	}
}

func (p *Pool) getNodeByFailoverOrder() (*node, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for i := 0; i < len(p.Nodes); i++ {
		if node, exists := p.Nodes[i]; exists && node.Healthy {
			return node, false
		}
	}
	return p.Fallback, true
}

func (p *Pool) getNodeByRoundRobin() (*node, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	for i := 0; i < len(p.Nodes); i++ {
		n := <-p.RoundRobinChan
		p.RoundRobinChan <- n
		if n.Healthy {
			return n, false
		}
	}
	return p.Fallback, true
}

func (p *Pool) getNodeByRandom() (*node, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var keys []int
	for k := range p.Nodes {
		keys = append(keys, k)
	}

	rand.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	for _, k := range keys {
		if p.Nodes[k].Healthy {
			return p.Nodes[k], false
		}
	}
	return p.Fallback, true
}

func (p *Pool) HandleRequest(ctx *fasthttp.RequestCtx) {
	req := &ctx.Request

	var jRPCReq jsonRPCPayload
	err := json.Unmarshal(ctx.PostBody(), &jRPCReq)
	if err != nil {
		ctx.Error("Failed to unmarshal request: "+err.Error(), 520)
		log.WithFields(logrus.Fields{
			"error":    err,
			"req_body": string(ctx.PostBody()),
		}).Error("Failed to unmarshal request.")
		go metrics.IncrementInvalidRequests()
		return
	}

	go metrics.IncrementTotalRequests(jRPCReq.Method)

	for {
		node, fallback := p.getHealthyNode()
		if fallback && node == nil {
			ctx.Error("No healthy nodes available", 521)
			log.Error("No healthy nodes available")
			return
		}

		log.WithFields(logrus.Fields{
			"node":        node.URI,
			"is_fallback": fallback,
		}).Debug("selected node")

		req.Header.Set("X-Forwarded-For", ctx.RemoteIP().String())
		ctx.Request.SetRequestURI(node.URI)

		ctx.Request.Header.VisitAll(func(key, value []byte) {
			req.Header.SetBytesKV(key, value)
		})

		if len(ctx.PostBody()) > 0 {
			req.SetBody(ctx.PostBody())
		}

		resp := fasthttp.AcquireResponse()
		err := node.Client.Do(req, resp)

		if err != nil {
			fasthttp.ReleaseResponse(resp)
			switch fallback {
			case true:
				ctx.Error("Request failed at fallback: "+err.Error(), 521)
				log.WithFields(logrus.Fields{"error": err}).Error("Request failed at fallback")
				return
			default:
				node.SetHealthy(false)
				log.WithFields(logrus.Fields{
					"node":    node.URI,
					"healthy": node.Healthy,
					"error":   err,
				}).Error("Request failed. Node set to unhealthy.")
				continue
			}
		}

		if resp.StatusCode() != 200 {
			statusCode := resp.StatusCode()
			respBody := resp.Body()
			fasthttp.ReleaseResponse(resp)
			switch fallback {
			case true:
				ctx.Error(fmt.Sprintf("Request failed at fallback, status: %d", statusCode), 521)
				log.WithFields(logrus.Fields{"http_status_code": statusCode}).Error("Request failed at fallback")
				return
			default:
				node.SetHealthy(false)
				log.WithFields(logrus.Fields{
					"node":             node.URI,
					"http_status_code": statusCode,
					"response_body":    string(respBody),
				}).Error("Request failed. Node set to unhealthy.")
				continue
			}
		}

		ctx.Response.SetStatusCode(resp.StatusCode())
		ctx.Response.SetBody(resp.Body())
		resp.Header.VisitAll(func(key, value []byte) {
			ctx.Response.Header.SetBytesKV(key, value)
		})
		fasthttp.ReleaseResponse(resp)
		log.Info("Request succeeded")
		return
	}
}

// StartHealthCheckLoop periodically checks node health, allowing recovery for lagging nodes.
func (p *Pool) StartHealthCheckLoop(frequency int) {
	const blockLagTolerance int64 = 50

	log.Infof("Starting health check loop: frequency=%ds, lag_tolerance=%d", frequency, blockLagTolerance)

	for {
		p.mu.RLock()
		nodesToCheck := make([]*node, 0, len(p.Nodes))
		for _, n := range p.Nodes {
			nodesToCheck = append(nodesToCheck, n)
		}
		p.mu.RUnlock()

		var maxHeight int64 = 0
		heightCheckSuccess := make(map[*node]bool, len(nodesToCheck))

		// Pass 1: Fetch heights, handle fetch errors
		for _, n := range nodesToCheck {
			height, err := GetBlockHeight(n.URI)
			if err != nil {
				log.WithFields(logrus.Fields{"error": err, "node": n.URI}).Warn("HealthCheck: GetBlockHeight failed")
				n.SetHealthy(false)
				heightCheckSuccess[n] = false
			} else {
				n.SetHeight(height)
				heightCheckSuccess[n] = true
				if height > maxHeight {
					maxHeight = height
				}
				metrics.BlockHeight.Set(float64(height))
			}
		}

		// Pass 2: Evaluate lag for nodes that successfully reported height
		log.Debugf("HealthCheck: Max height found: %d", maxHeight)
		for _, n := range nodesToCheck {
			if success := heightCheckSuccess[n]; success {
				if maxHeight > 0 && (maxHeight-n.Height) > blockLagTolerance {
					// Node is lagging
					n.SetHealthy(false)
					log.WithFields(logrus.Fields{
						"node": n.URI, "height": n.Height, "maxHeight": maxHeight, "lag": maxHeight - n.Height,
					}).Warn("HealthCheck: Node lagging")
				} else {
					// Node is not lagging, mark healthy (enables recovery)
					n.SetHealthy(true)
				}
			}
		}
		log.Debug("HealthCheck: Cycle finished.")

		time.Sleep(time.Duration(frequency) * time.Second)
	}
}

type jsonRPCPayload struct {
	Jsonrpc string      `json:"jsonrpc"`
	ID      int         `json:"id"`
	Method  string      `json:"method,omitempty"`
	Params  interface{} `json:"params,omitempty"`
	Result  interface{} `json:"result,omitempty"`
	Error   interface{} `json:"error,omitempty"`
}

func GetBlockHeight(target string) (int64, error) {
	request := jsonRPCPayload{
		Jsonrpc: "2.0",
		ID:      1,
		Method:  "eth_blockNumber",
		Params:  []string{},
	}

	requestBody, err := json.Marshal(request)
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequest("POST", target, bytes.NewBuffer(requestBody))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		return 0, fmt.Errorf("http_status_code: %d", resp.StatusCode)
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, err
	}

	var result map[string]interface{}
	err = json.Unmarshal(respBody, &result)
	if err != nil {
		return 0, err
	}

	if result["error"] != nil {
		return 0, fmt.Errorf("rpc error: %v", result["error"])
	}
	if result["result"] == nil {
		return 0, fmt.Errorf("rpc result is null")
	}

	resultStr, ok := result["result"].(string)
	if !ok {
		return 0, fmt.Errorf("rpc result is not a string: %T", result["result"])
	}

	blockHeight, err := hexStringToInt64(resultStr)
	if err != nil {
		return 0, err
	}

	return blockHeight, nil
}

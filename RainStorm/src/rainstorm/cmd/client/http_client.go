// rainstorm/client/http_client.go
package client

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"rainstorm-c7/config"
	"rainstorm-c7/hydfs/storage"
	"rainstorm-c7/rainstorm/core"
	http_utils "rainstorm-c7/rainstorm/utils"
	generic_utils "rainstorm-c7/utils"
	"strings"
	"sync"
	"time"
)

// ActionType is the type of plan action.
type ActionType string

const (
	ActionStart ActionType = "start"
	ActionStop  ActionType = "stop"
)

// PlanAction describes one Start/Stop to perform on a worker.
type PlanAction struct {
	Worker           core.WorkerInfo
	Task             core.TaskID
	Action           ActionType
	Operator         string   // binary name/path to run on worker (for starts)
	Args             []string // operator args (for starts)
	HyDFSLogFileName string   // HyDFS log file name (for starts)
	JobID            string   // Job ID (for starts)
}

// HTTPClient wraps an http.Client and knows how to send Start/Stop RPCs.
type HTTPClient struct {
	httpClient *http.Client
	Config     config.Config
}

// NewHTTPClient creates a new HTTPClient with a default underlying http.Client.
func NewHTTPClient(cfg config.Config) *HTTPClient {
	return &HTTPClient{
		httpClient: &http.Client{},
		Config:     cfg,
	}
}

// SendPlan executes the given plan: each Start/Stop is sent in its own goroutine.
type ActionResult struct {
	Action PlanAction
	Err    error
}

// SendPlan executes the given plan: each Start/Stop is sent in its own goroutine.
// It waits for all to finish and returns a result per action.
func (c *HTTPClient) SendPlan(plan []PlanAction) []ActionResult {
	results := make([]ActionResult, 0, len(plan))
	if len(plan) == 0 {
		return results
	}

	ch := make(chan ActionResult, len(plan))
	var wg sync.WaitGroup

	for _, step := range plan {
		step := step // capture loop var
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			var updatedWorkerInfo core.WorkerInfo
			switch step.Action {
			case ActionStart:
				updatedWorkerInfo, err = c.sendStartToWorker(step)
			case ActionStop:
				updatedWorkerInfo, err = c.sendStopToWorker(step)
			}
			updatedAction := PlanAction{
				Worker:           updatedWorkerInfo,
				Task:             step.Task,
				Action:           step.Action,
				Operator:         step.Operator,
				Args:             step.Args,
				HyDFSLogFileName: step.HyDFSLogFileName,
				JobID:            step.JobID,
			}
			result := ActionResult{Action: updatedAction, Err: err}
			ch <- result
		}()
	}

	wg.Wait()
	close(ch)

	for res := range ch {
		results = append(results, res)
	}
	return results
}

// sendStart builds a StartTaskRequest and POSTs it to /start_task on the worker.
func (c *HTTPClient) sendStartToWorker(p PlanAction) (core.WorkerInfo, error) {
	if p.Worker.NodeID.NodeIDToString() == "" {
		err := fmt.Errorf("worker %v has empty NodeID, cannot start task %+v", p.Worker.NodeID, p.Task)
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] %v\n"+generic_utils.Reset, err)
		return core.WorkerInfo{}, err
	}

	req := core.StartTaskRequest{
		TaskID:           p.Task,
		Operator:         p.Operator,
		Args:             p.Args,
		HyDFSLogFileName: p.HyDFSLogFileName,
		JobID:            p.JobID,
	}

	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to marshal StartTaskRequest for %+v: %v\n"+generic_utils.Reset, p.Task, err)
		return core.WorkerInfo{}, err
	}

	workerEndpoint, err := http_utils.ResolveReplicaEndpoint(p.Worker.NodeID, c.Config)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to resolve endpoint for worker %v: %v\n"+generic_utils.Reset, p.Worker.NodeID, err)
		return core.WorkerInfo{}, err
	}
	url := fmt.Sprintf("%s/worker/start_task", workerEndpoint)
	url = http_utils.EnsureHTTPBase(url)
	log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] sending start_task to %s for task %+v\n"+generic_utils.Reset, url, p.Task)

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] http POST %s failed for task %+v: %v\n"+generic_utils.Reset, url, p.Task, err)
		return core.WorkerInfo{}, err
	}
	defer resp.Body.Close()

	var respBody core.StartTaskResponse

	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to decode StartTaskResponse for %+v: %v\n"+generic_utils.Reset, p.Task, err)
		return core.WorkerInfo{}, err
	}

	if resp.StatusCode != http.StatusOK || !respBody.OK {
		err := fmt.Errorf("start_task on %s returned %s for task %+v: %s",
			p.Worker.NodeID.NodeIDToString(), resp.Status, p.Task, respBody.Err)
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] %v\n"+generic_utils.Reset, err)
		return core.WorkerInfo{}, err
	}

	log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] successfully started task %+v on worker %v\n"+generic_utils.Reset, p.Task, p.Worker.NodeID)
	return respBody.WorkerInfo, nil
}

// sendStop builds a StopTaskRequest and POSTs it to /stop_task on the worker.
func (c *HTTPClient) sendStopToWorker(p PlanAction) (core.WorkerInfo, error) {
	if p.Worker.NodeID.NodeIDToString() == "" {
		err := fmt.Errorf("worker %v has empty NodeID, cannot stop task %+v", p.Worker.NodeID, p.Task)
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] %v\n"+generic_utils.Reset, err)
		return core.WorkerInfo{}, err
	}

	req := core.StopTaskRequest{TaskID: p.Task, JobID: p.JobID}

	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to marshal StopTaskRequest for %+v: %v\n"+generic_utils.Reset, p.Task, err)
		return core.WorkerInfo{}, err
	}

	workerEndpoint, err := http_utils.ResolveReplicaEndpoint(p.Worker.NodeID, c.Config)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to resolve endpoint for worker %v: %v\n"+generic_utils.Reset, p.Worker.NodeID, err)
		return core.WorkerInfo{}, err
	}
	url := fmt.Sprintf("%s/worker/stop_task", workerEndpoint)
	url = http_utils.EnsureHTTPBase(url)
	log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] sending stop_task to %s for task %+v\n"+generic_utils.Reset, url, p.Task)

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] http POST %s failed for stop task %+v: %v\n"+generic_utils.Reset, url, p.Task, err)
		return core.WorkerInfo{}, err
	}
	defer resp.Body.Close()

	var respBody core.StopTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to decode StopTaskResponse for %+v: %v\n"+generic_utils.Reset, p.Task, err)
		return core.WorkerInfo{}, err
	}

	if resp.StatusCode != http.StatusOK || !respBody.OK {
		err := fmt.Errorf("stop_task on %s returned %s for task %+v: %s",
			p.Worker.NodeID.NodeIDToString(), resp.Status, p.Task, respBody.Err)
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] %v\n"+generic_utils.Reset, err)
		return core.WorkerInfo{}, err
	}

	log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] successfully stopped task %+v on worker %v\n"+generic_utils.Reset, p.Task, p.Worker.NodeID)
	return respBody.WorkerInfo, nil
}

func (c *HTTPClient) KillTaskOnWorker(worker core.WorkerInfo, taskID core.TaskID) (core.KilledTaskResponse, error) {
	req := core.KillTaskRequest{
		TaskID: taskID,
	}
	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to marshal KillTaskRequest for %+v: %v\n"+generic_utils.Reset, taskID, err)
		return core.KilledTaskResponse{}, err
	}

	workerEndpoint, err := http_utils.ResolveReplicaEndpoint(worker.NodeID, c.Config)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to resolve endpoint for worker %v: %v\n"+generic_utils.Reset, worker.NodeID, err)
		return core.KilledTaskResponse{}, err
	}
	url := fmt.Sprintf("%s/worker/kill_task", workerEndpoint)
	url = http_utils.EnsureHTTPBase(url)
	log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] sending kill_task to %s for task %+v\n"+generic_utils.Reset, url, taskID)
	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] http POST %s failed for kill task %+v: %v\n"+generic_utils.Reset, url, taskID, err)
		return core.KilledTaskResponse{}, err
	}
	defer resp.Body.Close()

	var respBody core.KilledTaskResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to decode KilledTaskResponse for %+v: %v\n"+generic_utils.Reset, taskID, err)
		return core.KilledTaskResponse{}, err
	}

	if resp.StatusCode != http.StatusOK || !respBody.OK {
		err := fmt.Errorf("kill_task on %s returned %s for task %+v: %s",
			worker.NodeID.NodeIDToString(), resp.Status, taskID, respBody.Err)
		log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] %v\n"+generic_utils.Reset, err)
		return core.KilledTaskResponse{}, err
	}

	log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] successfully killed task %+v on worker %v\n"+generic_utils.Reset, taskID, worker.NodeID)
	return respBody, nil
}

func (c *HTTPClient) BroadcastTaskMap(workers []core.WorkerInfo, taskMap map[int]map[int]core.WorkerInfo, taskMapVersion int64) {
	log.Printf(generic_utils.Yellow + "[rainstorm-leader-http-client] broadcasting updated task map to all workers\n" + generic_utils.Reset)
	for _, worker := range workers {
		// Send to each worker in separate goroutine
		go func(w core.WorkerInfo) {
			req := core.TaskMapPayload{
				TaskMap:        taskMap,
				TaskMapVersion: taskMapVersion,
			}

			// Pretty print the task map payload created
			log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] broadcasting task map to worker %v\n"+generic_utils.Reset, w.NodeID)

			body, err := json.Marshal(&req)
			if err != nil {
				log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to marshal task map for worker %v: %v\n"+generic_utils.Reset, w.NodeID, err)
				return
			}

			workerEndpoint, err := http_utils.ResolveReplicaEndpoint(w.NodeID, c.Config)
			if err != nil {
				log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] failed to resolve endpoint for worker %v: %v\n"+generic_utils.Reset, w.NodeID, err)
				return
			}

			url := fmt.Sprintf("%s/worker/task_map", workerEndpoint)
			url = http_utils.EnsureHTTPBase(url)
			log.Printf(generic_utils.Pink+"[rainstorm-leader-http-client] sending task_map to %s\n"+generic_utils.Reset, url)

			resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
			if err != nil {
				log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] http POST %s failed for worker %v: %v\n"+generic_utils.Reset, url, w.NodeID, err)
				return
			}
			defer resp.Body.Close()

			if resp.StatusCode != http.StatusOK {
				log.Printf(generic_utils.Red+"[rainstorm-leader-http-client] task_map request to %s returned %s for worker %v\n"+generic_utils.Reset,
					workerEndpoint, resp.Status, w.NodeID)
			}
		}(worker)
	}
}

func (c *HTTPClient) BroadcastEOSToNextStageTasks(tupleID core.TupleID, nextStage int, taskMap map[int]map[int]core.WorkerInfo, taskMapVersion int64) {
	log.Printf(generic_utils.Yellow + "[rainstorm-http-client] broadcasting EOS to next stage tasks\n" + generic_utils.Reset)
	for tidx, worker := range taskMap[nextStage] {
		// Send to each worker in separate goroutine
		go func(w core.WorkerInfo) {
			t := core.Tuple{
				ID:   tupleID,
				Type: core.EOS,
			}
			forTaskID := core.TaskID{
				Stage:     nextStage,
				TaskIndex: tidx,
			}

			req := core.DeliverTupleRequest{
				TaskID:         forTaskID,
				Tuple:          t,
				TaskMapVersion: taskMapVersion,
			}

			body, err := json.Marshal(&req)
			if err != nil {
				log.Printf(generic_utils.Red+"[rainstorm-http-client] failed to marshal EOSTaskRequest for worker %v: %v\n"+generic_utils.Reset, w.NodeID, err)
				return
			}

			workerEndpoint, err := http_utils.ResolveReplicaEndpoint(w.NodeID, c.Config)
			if err != nil {
				log.Printf(generic_utils.Red+"[rainstorm-http-client] failed to resolve endpoint for worker %v: %v\n"+generic_utils.Reset, w.NodeID, err)
				return
			}

			url := fmt.Sprintf("%s/worker/deliver_tuple", workerEndpoint)
			url = http_utils.EnsureHTTPBase(url)
			log.Printf(generic_utils.Pink+"[rainstorm-http-client] sending EOS to %s for task %+v\n"+generic_utils.Reset, url, forTaskID)

			resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))

			if err != nil {
				log.Printf(generic_utils.Red+"[rainstorm-http-client] http POST %s failed for worker %v: %v\n"+generic_utils.Reset, url, w.NodeID, err)
				return
			}
			defer resp.Body.Close()

			// If we receive an error in the response, log it but don't cause any side effects.
			var respBody core.DeliverTupleResponse
			if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
				log.Printf(generic_utils.Red+"[rainstorm-http-client] failed to decode DeliverTupleResponse while delivering EOS for %+v: %v\n"+generic_utils.Reset, forTaskID, err)
				return
			}

			if resp.StatusCode != http.StatusOK {
				log.Printf(generic_utils.Red+"[rainstorm-http-client] EOS request to %s returned %s for worker %v\n"+generic_utils.Reset,
					workerEndpoint, resp.Status, w.NodeID)
			}

			log.Printf(generic_utils.Pink+"[rainstorm-http-client] successfully sent EOS for task %+v to worker %v\n"+generic_utils.Reset, forTaskID, worker.NodeID)

		}(worker)
	}
}

// notifyLeaderTaskFailed sends a TaskFailedRequest to the leader's /task_failed endpoint.
func (c *HTTPClient) NotifyLeaderTaskFailed(taskID core.TaskID, workerInfo core.WorkerInfo, exitErr error) {
	req := core.TaskFailedRequest{
		TaskID:     taskID,
		WorkerInfo: workerInfo,
		Reason:     exitErr.Error(),
	}

	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to marshal TaskFailedRequest: %v\n"+generic_utils.Reset, err)
		return
	}

	if c.Config.Introducer == "" {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] leaderAddr is empty, cannot notify failure for %+v\n"+generic_utils.Reset, taskID)
		return
	}

	// Split c.Config.Introducer on :, and change the port to Rainstorm HTTP port
	// We get a domain name and not an IP address here.
	introducerCName := strings.Split(c.Config.Introducer, ":")[0]
	if introducerCName == "" {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to split Introducer address %s: %v\n"+generic_utils.Reset, c.Config.Introducer, err)
		return
	}

	leaderEndpoint := introducerCName + c.Config.RainstormHTTP

	url := fmt.Sprintf("%s/leader/task_failed", leaderEndpoint)
	url = http_utils.EnsureHTTPBase(url)
	log.Printf(generic_utils.Pink+"[rainstorm-worker-agent-http-client] sending task_failed to %s for task %+v\n"+generic_utils.Reset, url, taskID)

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] http POST %s failed for task_failed %+v: %v\n"+generic_utils.Reset, url, taskID, err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] task_failed request to %s returned %s for task %+v\n"+generic_utils.Reset,
			leaderEndpoint, resp.Status, taskID)
	}
}

func (c *HTTPClient) NotifyLeaderFinalStageTaskCompleted(taskID core.TaskID, workerInfo core.WorkerInfo) error {
	req := core.FinalStageTaskCompletedRequest{
		TaskID:     taskID,
		WorkerInfo: workerInfo,
	}

	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to marshal FinalStageTaskCompletedRequest for %+v: %v\n"+generic_utils.Reset, taskID, err)
		return err
	}

	if c.Config.Introducer == "" {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] leaderAddr is empty, cannot notify final stage task completion for %+v\n"+generic_utils.Reset, taskID)
		return fmt.Errorf("leaderAddr is empty, cannot notify final stage task completion for %+v", taskID)
	}

	// Split c.Config.Introducer on :, and change the port to Rainstorm HTTP port
	// We get a domain name and not an IP address here.
	introducerCName := strings.Split(c.Config.Introducer, ":")[0]
	if introducerCName == "" {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to split Introducer address %s: %v\n"+generic_utils.Reset, c.Config.Introducer, err)
		return fmt.Errorf("failed to split Introducer address %s: %w", c.Config.Introducer, err)
	}

	leaderEndpoint := introducerCName + c.Config.RainstormHTTP

	url := fmt.Sprintf("%s/leader/final_stage_task_completed", leaderEndpoint)
	url = http_utils.EnsureHTTPBase(url)
	log.Printf(generic_utils.Pink+"[rainstorm-worker-agent-http-client] sending final_stage_task_completed to %s for task %+v\n"+generic_utils.Reset, url, taskID)

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] http POST %s failed for final_stage_task_completed %+v: %v\n"+generic_utils.Reset, url, taskID, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] final_stage_task_completed request to %s returned %s for task %+v\n"+generic_utils.Reset,
			leaderEndpoint, resp.Status, taskID)
		return fmt.Errorf("final_stage_task_completed request to %s returned %s for task %+v",
			leaderEndpoint, resp.Status, taskID)
	}
	return nil
}

// ReportTaskMetrics sends per-task input rate to the leader.
func (c *HTTPClient) ReportTaskMetrics(taskID core.TaskID, rate float64) error {
	req := core.TaskMetricsReportRequest{
		TaskID:    taskID,
		InputRate: rate,
	}

	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+
			"[rainstorm-worker-agent-http-client] failed to marshal TaskMetricsReportRequest for %+v: %v\n"+
			generic_utils.Reset, taskID, err)
		return err
	}

	if c.Config.Introducer == "" {
		log.Printf(generic_utils.Red+
			"[rainstorm-worker-agent-http-client] Introducer is empty, cannot report metrics for %+v\n"+
			generic_utils.Reset, taskID)
		return fmt.Errorf("no introducer configured")
	}

	// derive leader endpoint from Introducer.
	introducerCName := strings.Split(c.Config.Introducer, ":")[0]
	if introducerCName == "" {
		log.Printf(generic_utils.Red+
			"[rainstorm-worker-agent-http-client] failed to split Introducer address %s\n"+
			generic_utils.Reset, c.Config.Introducer)
		return fmt.Errorf("invalid introducer address: %s", c.Config.Introducer)
	}

	leaderEndpoint := introducerCName + c.Config.RainstormHTTP

	url := fmt.Sprintf("%s/leader/task_metrics", leaderEndpoint)
	url = http_utils.EnsureHTTPBase(url)

	log.Printf(generic_utils.Pink+
		"[rainstorm-worker-agent-http-client] sending task_metrics to %s for task %+v (rate=%.2f)\n"+
		generic_utils.Reset, url, taskID, rate)

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf(generic_utils.Red+
			"[rainstorm-worker-agent-http-client] http POST %s failed for task_metrics %+v: %v\n"+
			generic_utils.Reset, url, taskID, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf(generic_utils.Red+
			"[rainstorm-worker-agent-http-client] task_metrics request to %s returned %s for task %+v\n"+
			generic_utils.Reset, leaderEndpoint, resp.Status, taskID)
		return fmt.Errorf("task_metrics returned %s", resp.Status)
	}

	return nil
}

func (c *HTTPClient) SendTuple(toWorker core.WorkerInfo, forTaskID core.TaskID, t core.Tuple, taskMapVersion int64) error {
	req := core.DeliverTupleRequest{
		TaskID:         forTaskID, // Contains information about which task on the worker to deliver to
		Tuple:          t,         // Contains information about who the source of the tuple was along with key and value
		TaskMapVersion: taskMapVersion,
	}

	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to marshal DeliverTupleRequest for task %+v: %v\n"+generic_utils.Reset, forTaskID, err)
		return err
	}

	workerEndpoint, err := http_utils.ResolveReplicaEndpoint(toWorker.NodeID, c.Config)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to resolve endpoint for worker %v: %v\n"+generic_utils.Reset, toWorker.NodeID, err)
		return err
	}

	url := fmt.Sprintf("%s/worker/deliver_tuple", workerEndpoint)
	url = http_utils.EnsureHTTPBase(url)
	log.Printf(generic_utils.Pink+"[rainstorm-worker-agent-http-client] sending deliver_tuple to %s for task %+v\n"+generic_utils.Reset, url, forTaskID)

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))

	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] http POST %s failed for deliver_tuple %+v: %v\n"+generic_utils.Reset, url, forTaskID, err)
		return err
	}
	defer resp.Body.Close()

	// If non-200, log and treat as error BEFORE decoding.
	if resp.StatusCode != http.StatusOK {
		log.Printf("[http-client] deliver_tuple got non-200 %d body=%q", resp.StatusCode, string(body))
		return fmt.Errorf("deliver_tuple non-200 %d: %s", resp.StatusCode, string(body))
	}

	// If we receive an error in the response, log it but don't cause any side effects.
	var respBody core.DeliverTupleResponse
	if err := json.NewDecoder(resp.Body).Decode(&respBody); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to decode DeliverTupleResponse for %+v: %v\n"+generic_utils.Reset, forTaskID, err)
		return err
	}
	if resp.StatusCode != http.StatusOK || !respBody.OK {
		err := fmt.Errorf("deliver_tuple on %s returned %s for task %+v: %s",
			toWorker.NodeID.NodeIDToString(), resp.Status, forTaskID, respBody.Err)
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] %v\n"+generic_utils.Reset, err)
		return err
	}
	log.Printf(generic_utils.Pink+"[rainstorm-worker-agent-http-client] successfully delivered tuple for task %+v to worker %v\n"+generic_utils.Reset, forTaskID, toWorker.NodeID)
	return nil
}

func (c *HTTPClient) SendTupleAck(toWorker core.WorkerInfo, tupleID core.TupleID, ackForSource bool) error {
	req := core.TupleAckRequest{
		TupleID: tupleID, // which tuple is done
	}
	body, err := json.Marshal(&req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to marshal TupleAckRequest for tupleID %+v: %v\n"+generic_utils.Reset, tupleID, err)
		return err
	}

	destinationEndpoint := ""
	if !ackForSource && toWorker.NodeID.NodeIDToString() != "" {
		destinationEndpoint, err = http_utils.ResolveReplicaEndpoint(toWorker.NodeID, c.Config)
		if err != nil {
			log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to resolve endpoint for worker %v: %v\n"+generic_utils.Reset, toWorker.NodeID, err)
			return err
		}
	} else {
		if c.Config.Introducer == "" {
			log.Printf(generic_utils.Red+"[agent] leaderAddr is empty, cannot send ack for tupleID: %+v\n"+generic_utils.Reset, tupleID)
			return fmt.Errorf("leaderAddr is empty, cannot send ack for tupleID: %+v", tupleID)
		}
		introducerCName := strings.Split(c.Config.Introducer, ":")[0]
		if introducerCName == "" {
			log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] failed to split Introducer address %s: %v\n"+generic_utils.Reset, c.Config.Introducer, err)
			return fmt.Errorf("failed to split Introducer address %s: %w", c.Config.Introducer, err)
		}

		destinationEndpoint = introducerCName + c.Config.RainstormHTTP
	}

	url := fmt.Sprintf("%s/worker/tuple_ack", destinationEndpoint)
	url = http_utils.EnsureHTTPBase(url)
	log.Printf(generic_utils.Pink+"[rainstorm-worker-agent-http-client] sending tuple_ack to %s for tupleID %+v\n"+generic_utils.Reset, url, tupleID)

	resp, err := c.httpClient.Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] http POST %s failed for tuple_ack %+v: %v\n"+generic_utils.Reset, url, tupleID, err)
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		log.Printf(generic_utils.Red+"[rainstorm-worker-agent-http-client] tuple_ack request to %s returned %s for tupleID %+v\n"+generic_utils.Reset,
			destinationEndpoint, resp.Status, tupleID)
	}
	return nil
}

// ------------------ HyDFS Log Processing ----------------------------
func (c *HTTPClient) GetHyDFSFileContent(hyDFSFileName string) ([]byte, error) {
	// Hit the '/v1/user/files/content?hydfs_file_name=<HyDFSLogFileName>&create_local_file=<false>' endpoint. Don't send a local_file_name param.
	// 1) derive min_replies from consistency level
	level := "quorum"
	minReplies, err := c.minRepliesFor(level)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] invalid consistency level %q: %v\n"+generic_utils.Reset, level, err)
		return nil, fmt.Errorf("invalid consistency level %q: %w", level, err)
	}

	// 2) build request URL
	base := http_utils.EnsureHTTPBase(c.Config.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/user/files/content?hydfs_file_name=%s&create_local_file=false&min_replies=%d",
		base, mustURLEncode(hyDFSFileName), minReplies)

	log.Printf(generic_utils.Pink+"[rainstorm-http-client] Issuing GET %s\n"+generic_utils.Reset, u)

	// 3) issue GET
	resp, err := c.httpClient.Get(u)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] http GET %s failed: %v\n"+generic_utils.Reset, u, err)
		return nil, fmt.Errorf("http GET %s failed: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Printf(generic_utils.Red+"[rainstorm-http-client] server returned error for GET %s: %s\n"+generic_utils.Reset, u, strings.TrimSpace(string(b)))
		return nil, fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// 4) decode server response
	type manBrief struct {
		Addr       string    `json:"addr"`
		Version    uint64    `json:"version"`
		LastUpdate time.Time `json:"last_update"`
	}

	var out struct {
		FileName       string     `json:"file_name"`
		LocalFileName  string     `json:"local_file_name"`
		FileID         string     `json:"file_id"`
		ChosenReplica  string     `json:"chosen_replica"`
		ChosenVersion  uint64     `json:"chosen_version"`
		ChosenUpdated  time.Time  `json:"chosen_last_update"`
		FileContent    []byte     `json:"file_content"`
		Bytes          int64      `json:"bytes_written"`
		Quorum         int        `json:"manifest_quorum"`
		ManifestsUsed  []manBrief `json:"manifests_used"`
		ManifestErrors []string   `json:"manifest_errors,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] decode response failed for GET %s: %v\n"+generic_utils.Reset, u, err)
		return nil, fmt.Errorf("decode response: %w", err)
	}

	// 5) print a tidy summary
	log.Printf("get %q (min_replies=%d)\n", hyDFSFileName, out.Quorum)
	log.Printf("  chosen replica: %s\n", out.ChosenReplica)
	log.Printf("  file_id=%s version=%d bytes=%d updated=%s\n",
		out.FileID, out.ChosenVersion, out.Bytes, out.ChosenUpdated.Format(time.RFC3339Nano))

	if len(out.ManifestsUsed) > 0 {
		log.Println("  manifests used:")
		for _, m := range out.ManifestsUsed {
			log.Printf("    - %s  ver=%d  last_update=%s\n",
				m.Addr, m.Version, m.LastUpdate.Format(time.RFC3339Nano))
		}
	}
	if len(out.ManifestErrors) > 0 {
		log.Println("  manifest errors:")
		for _, e := range out.ManifestErrors {
			log.Printf("    - %s\n", e)
		}
	}
	return out.FileContent, nil
}

func (c *HTTPClient) CreateHyDFSFileWithContent(hyDFSFileName string, content []byte) error {
	// 1) derive min_replies from consistency level
	level := "quorum"
	minReplies, err := c.minRepliesFor(level)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] invalid consistency level %q: %v\n"+generic_utils.Reset, level, err)
		return fmt.Errorf("invalid consistency level %q: %w", level, err)
	}

	// 2) build request URL
	base := http_utils.EnsureHTTPBase(c.Config.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/user/create_with_data?hydfs_file_name=%s&min_replies=%d",
		base, mustURLEncode(hyDFSFileName), minReplies)
	log.Printf(generic_utils.Pink+"[rainstorm-http-client] Issuing POST %s\n"+generic_utils.Reset, u)

	// 3) issue POST
	resp, err := c.httpClient.Post(u, "application/octet-stream", bytes.NewReader(content))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] http POST %s failed: %v\n"+generic_utils.Reset, u, err)
		return fmt.Errorf("http POST %s failed: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Printf(generic_utils.Red+"[rainstorm-http-client] server returned error for POST %s: %s\n"+generic_utils.Reset, u, strings.TrimSpace(string(b)))
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// 4) Decode JSON response
	type reply struct {
		Addr   string                `json:"addr"`
		Status int                   `json:"status"`
		Result *storage.FileOpResult `json:"result,omitempty"`
		Err    string                `json:"err,omitempty"`
		Self   bool                  `json:"self"`
	}
	var out struct {
		Quorum   int     `json:"quorum"`
		Received []reply `json:"received"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] decode response failed for POST %s: %v\n"+generic_utils.Reset, u, err)
		return fmt.Errorf("decode response: %w", err)
	}

	// 5) Print result summary
	log.Printf(generic_utils.Pink+"[rainstorm-http-client] Created %q (min_replies=%d)\n"+generic_utils.Reset, hyDFSFileName, out.Quorum)
	for i, r := range out.Received {
		if r.Result != nil {
			res := r.Result
			log.Printf(generic_utils.Pink+"[rainstorm-http-client]  [%d] %-20s status=%d self=%v\n"+generic_utils.Reset, i, generic_utils.ResolveDNSFromIP(r.Addr), r.Status, r.Self)
			log.Printf(generic_utils.Pink+"[rainstorm-http-client]       file_token=%s version=%d bytes=%d\n"+generic_utils.Reset, res.FileToken, res.Version, res.Bytes)
			log.Printf(generic_utils.Pink+"[rainstorm-http-client]       op_id=%s ts=%s client=%s seq=%d\n"+generic_utils.Reset,
				res.OpID, res.Timestamp.Format(time.RFC3339Nano), res.ClientID, res.ClientSeq)
		} else {
			log.Printf(generic_utils.Red+"[rainstorm-http-client]  [%d] %-20s status=%d self=%v err=%s\n"+generic_utils.Reset, i, generic_utils.ResolveDNSFromIP(r.Addr), r.Status, r.Self, r.Err)
		}
	}
	return nil
}

func (c *HTTPClient) AppendHyDFSFileContent(hyDFSFileName string, content []byte) error {
	// 1) derive min_replies from consistency level
	level := "quorum"
	minReplies, err := c.minRepliesFor(level)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] invalid consistency level %q: %v\n"+generic_utils.Reset, level, err)
		return fmt.Errorf("invalid consistency level %q: %w", level, err)
	}

	// 2) build request URL
	base := http_utils.EnsureHTTPBase(c.Config.HydfsHTTP)
	u := fmt.Sprintf("%s/v1/user/append?hydfs_file_name=%s&min_replies=%d",
		base, mustURLEncode(hyDFSFileName), minReplies)
	log.Printf(generic_utils.Pink+"[rainstorm-http-client] Issuing POST %s\n"+generic_utils.Reset, u)

	// 3) issue POST
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(content))
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] create http POST %s request failed: %v\n"+generic_utils.Reset, u, err)
		return fmt.Errorf("create http POST %s request failed: %w", u, err)
	}
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] http POST %s failed: %v\n"+generic_utils.Reset, u, err)
		return fmt.Errorf("http POST %s failed: %w", u, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		log.Printf(generic_utils.Red+"[rainstorm-http-client] server returned error for POST %s: %s\n"+generic_utils.Reset, u, strings.TrimSpace(string(b)))
		return fmt.Errorf("server: %s", strings.TrimSpace(string(b)))
	}

	// 4) Decode JSON response
	type reply struct {
		Addr   string                `json:"addr"`
		Status int                   `json:"status"`
		Result *storage.FileOpResult `json:"result,omitempty"`
		Err    string                `json:"err,omitempty"`
		Self   bool                  `json:"self"`
	}
	var out struct {
		Quorum   int     `json:"quorum"`
		Received []reply `json:"received"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		log.Printf(generic_utils.Red+"[rainstorm-http-client] decode response failed for POST %s: %v\n"+generic_utils.Reset, u, err)
		return fmt.Errorf("decode response: %w", err)
	}

	// 5) Print result summary
	fmt.Printf("Created %q (min_replies=%d)\n", hyDFSFileName, out.Quorum)
	for i, r := range out.Received {
		if r.Result != nil {
			res := r.Result
			fmt.Printf("  [%d] %-20s status=%d self=%v\n", i, generic_utils.ResolveDNSFromIP(r.Addr), r.Status, r.Self)
			fmt.Printf("       file_token=%s version=%d bytes=%d\n", res.FileToken, res.Version, res.Bytes)
			fmt.Printf("       op_id=%s ts=%s client=%s seq=%d\n",
				res.OpID, res.Timestamp.Format(time.RFC3339Nano), res.ClientID, res.ClientSeq)
		} else {
			fmt.Printf("  [%d] %-20s status=%d self=%v err=%s\n", i, generic_utils.ResolveDNSFromIP(r.Addr), r.Status, r.Self, r.Err)
		}
	}
	return nil
}

func (c *HTTPClient) minRepliesFor(level string) (int, error) {
	rf := c.Config.ReplicationFactor
	if rf <= 0 {
		return 0, fmt.Errorf("invalid replication factor: %d", rf)
	}
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "one":
		return 1, nil
	case "quorum":
		// strict quorum
		return rf/2 + 1, nil
	case "all":
		return rf, nil
	default:
		return 0, fmt.Errorf("invalid consistency level %q (expected one|quorum|all)", level)
	}
}

func mustURLEncode(s string) string {
	return url.QueryEscape(s)
}

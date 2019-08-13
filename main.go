package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	v1alpha1 "github.com/argoproj/argo/pkg/apis/workflow/v1alpha1"
	versioned "github.com/argoproj/argo/pkg/client/clientset/versioned"
	wffactory "github.com/argoproj/argo/pkg/client/informers/externalversions"
	wfInformerV1 "github.com/argoproj/argo/pkg/client/informers/externalversions/workflow/v1alpha1"
	wflistercorev1 "github.com/argoproj/argo/pkg/client/listers/workflow/v1alpha1"

	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

//declaring global varaibles for config and error
var (
	config          *rest.Config
	err             error
	AddAction       = "Add"
	UpdateAction    = "Update"
	DeleteAction    = "Delete"
	RunningStatus   = "Runnning"
	ActionDelimiter = "$$$"
)

//ArgoWorkflowController struct contains a lister to list workflows from the sharedInformer, boolean varaible
//to chcek whether workflows are synced and a rate limiting queue to handle to updated workflows
type ArgoWorkflowController struct {
	wflister       wflistercorev1.WorkflowLister
	wfListerSynced cache.InformerSynced
	queue          workqueue.RateLimitingInterface
}

//NewArgoWorkflowController creates a new Argo workflow controller with the lister and synced and a rate limited queue
//also added event handler function to monitor workflow updates
func NewArgoWorkflowController(client *kubernetes.Clientset,
	wfInformer wfInformerV1.WorkflowInformer) *ArgoWorkflowController {
	workflowController := &ArgoWorkflowController{
		wflister:       wfInformer.Lister(),
		wfListerSynced: wfInformer.Informer().HasSynced,
		queue:          workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "workflowWatch"),
	}

	wfInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				key, _ := cache.MetaNamespaceKeyFunc(obj)
				log.Println("workflow Added: ", key)
				workflowController.PushWorkflowChangesToQueue(obj, AddAction)

			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				key, _ := cache.MetaNamespaceKeyFunc(newObj)
				log.Println("workflow Updated: ", key)
				workflowController.PushWorkflowChangesToQueue(newObj, UpdateAction)

			},
			DeleteFunc: func(obj interface{}) {
				key, _ := cache.MetaNamespaceKeyFunc(obj)
				log.Println("workflow Deleted: ", key)
				workflowController.PushWorkflowChangesToQueue(obj, DeleteAction)

			},
		},
	)

	return workflowController
}

func main() {

	os.Setenv("KUBECONFIG", "config.yaml")
	kubeconfig := ""
	flag.StringVar(&kubeconfig, "kubeconfig", kubeconfig, "kubeconfig file")
	flag.Parse()
	if kubeconfig == "" {
		kubeconfig = os.Getenv("KUBECONFIG")
	}

	if kubeconfig != "" {
		config, err = clientcmd.BuildConfigFromFlags("", kubeconfig)
	} else {
		config, err = rest.InClusterConfig()
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error creating client: %v", err)
		os.Exit(1)
	}

	//creating client from kubeconfig file
	client := kubernetes.NewForConfigOrDie(config)
	versionedInterface := versioned.NewForConfigOrDie(config)

	//creating a argo workflow informer
	workflowInformer := wffactory.NewSharedInformerFactory(versionedInterface, 2*time.Minute)
	ArgoWorkflowController := NewArgoWorkflowController(client, workflowInformer.Argoproj().V1alpha1().Workflows())

	//starting informer and controller
	workflowInformer.Start(nil)
	ArgoWorkflowController.Run(nil)
}

//Run method first sync all workflow changes into the cache and make sure that no action taken till the sync is complete
//once the sync is completed, calls the runworker method to start processing the changes
func (workflowController *ArgoWorkflowController) Run(stop <-chan struct{}) {

	var wg sync.WaitGroup

	defer func() {
		// make sure the work queue is shut down which will trigger workers to end
		log.Print("shutting down queue")
		workflowController.queue.ShutDown()

		// wait on the workers
		log.Print("shutting down workers")
		wg.Wait()

		log.Print("workers are all done")
	}()

	log.Print("waiting for cache sync")
	if !cache.WaitForCacheSync(
		stop,
		workflowController.wfListerSynced) {
		log.Print("timed out waiting for cache sync")
		return
	}
	log.Print("caches are synced")

	go func() {
		// runWorker will loop until "something bad" happens. wait.Until will
		// then rekick the worker after one second.
		wait.Until(workflowController.runWorker, time.Second, stop)
		wg.Done()
	}()

	// wait until we're told to stop
	log.Print("waiting for stop signal")
	<-stop
	log.Print("received stop signal")
}

//runWorker is a loop that gets executes until we're told to stop.
//processNextWorkItem will automatically wait until there's work available
func (workflowController *ArgoWorkflowController) runWorker() {
	for workflowController.processNextWorkItem() {
	}
}

//processNextWorkItem pull the next work item from queue.
// It should be a key we use to lookup something in a cache
func (workflowController *ArgoWorkflowController) processNextWorkItem() bool {

	key, quit := workflowController.queue.Get()
	if quit {
		return false
	}
	//once processed the key indicate in the queue
	defer workflowController.queue.Done(key)

	var keystr = key.(string)

	err := workflowController.ProcessWorkflow(keystr)
	if err == nil {
		// since processing is completed without any error so informing the queue to stop tracking for this key
		workflowController.queue.Forget(key)
		return true
	}

	// since there was a failure report the error
	runtime.HandleError(fmt.Errorf("processWorkflow failed with: %v", err))

	// since processing of this key is failed, requeue the item to work on later.
	workflowController.queue.AddRateLimited(key)

	return true
}

//ProcessWorkflow method retrieves the workflow from the cache
func (workflowController *ArgoWorkflowController) ProcessWorkflow(keywithAction string) error {

	rawWorkFlows, err := workflowController.wflister.List(labels.Everything())

	workflowKey := keywithAction
	workflowAction := UpdateAction

	if strings.Index(keywithAction, ActionDelimiter) != -1 {
		keys := strings.Split(keywithAction, ActionDelimiter)
		workflowAction = keys[0]
		workflowKey = keys[1]
	}

	var sb strings.Builder

	for _, ns := range rawWorkFlows {
		sb.Reset()
		sb.WriteString(ns.Namespace)
		sb.WriteString("/")
		sb.WriteString(ns.Name)

		//fmt.Printf("value = %s", sb.String())
		if strings.Compare(sb.String(), workflowKey) == 0 {
			workflowController.printValues(ns, workflowAction)
		}

	}

	return err
}

//PushWorkflowChangesToQueue pushes modified workflow's name to the queue
func (workflowController *ArgoWorkflowController) PushWorkflowChangesToQueue(obj interface{}, workflowAction string) {

	key, err := cache.MetaNamespaceKeyFunc(obj)

	var keyWithAction strings.Builder
	keyWithAction.WriteString(workflowAction)
	keyWithAction.WriteString(ActionDelimiter)
	keyWithAction.WriteString(key)

	if err != nil {
		log.Printf("onAdd: error getting key for %#v: %v", obj, err)
		runtime.HandleError(err)
	}

	workflowController.queue.Add(keyWithAction.String())
}

//TODO: printValues method needs to be replaced by actual DB CRUD method
//since team is working on CRUD method, this method is added to test whether controller is monitoring the workflow properly
func (workflowController *ArgoWorkflowController) printValues(obj1 *v1alpha1.Workflow, workflowAction string) {

	if strings.Compare(workflowAction, AddAction) == 0 {
		log.Printf("Added : Name = %s, Started = %s, Completed = %s, Phase = %s", obj1.ObjectMeta.Name, obj1.Status.StartedAt, obj1.Status.FinishedAt, obj1.Status.Phase)
	} else {
		nodes := obj1.Status.Nodes
		for child, childStstatus := range nodes {

			if childStstatus.Phase != v1alpha1.NodeRunning && childStstatus.Phase != v1alpha1.NodePending {
				log.Printf("Updated Node = %s , Child Status = %s, Child Started = %s, Child Finished = %s,Failed Reason = %s", child, childStstatus.Phase, childStstatus.StartedAt, childStstatus.FinishedAt, childStstatus.Message)
			}

		}

	}

}

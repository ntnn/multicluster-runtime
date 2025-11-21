/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"log"
	"sync"

	"k8s.io/client-go/util/workqueue"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/source"

	mcmanager "sigs.k8s.io/multicluster-runtime/pkg/manager"
	"sigs.k8s.io/multicluster-runtime/pkg/multicluster"
	mcreconcile "sigs.k8s.io/multicluster-runtime/pkg/reconcile"
	mcsource "sigs.k8s.io/multicluster-runtime/pkg/source"
)

// Controller implements a Kubernetes API.  A Controller manages a work queue fed reconcile.Requests
// from source.Sources.  Work is performed through the reconcile.Reconciler for each enqueued item.
// Work typically is reads and writes Kubernetes objects to make the system state match the state specified
// in the object Spec.
type Controller = TypedController[mcreconcile.Request]

// Options are the arguments for creating a new Controller.
type Options = controller.TypedOptions[mcreconcile.Request]

// TypedController implements an API.
type TypedController[request mcreconcile.ClusterAware[request]] interface {
	controller.TypedController[request]
	multicluster.Aware

	// MultiClusterWatch watches the provided Source.
	MultiClusterWatch(src mcsource.TypedSource[client.Object, request]) error
}

// New returns a new Controller registered with the Manager.  The Manager will ensure that shared Caches have
// been synced before the Controller is Started.
//
// The name must be unique as it is used to identify the controller in metrics and logs.
func New(name string, mgr mcmanager.Manager, options Options) (Controller, error) {
	return NewTyped(name, mgr, options)
}

// NewTyped returns a new typed controller registered with the Manager,
//
// The name must be unique as it is used to identify the controller in metrics and logs.
func NewTyped[request mcreconcile.ClusterAware[request]](name string, mgr mcmanager.Manager, options controller.TypedOptions[request]) (TypedController[request], error) {
	c, err := NewTypedUnmanaged(name, mgr, options)
	if err != nil {
		return nil, err
	}

	// Add the controller as a Manager components
	return c, mgr.Add(c)
}

// NewUnmanaged returns a new controller without adding it to the manager. The
// caller is responsible for starting the returned controller.
//
// The name must be unique as it is used to identify the controller in metrics and logs.
func NewUnmanaged(name string, mgr mcmanager.Manager, options Options) (Controller, error) {
	return NewTypedUnmanaged[mcreconcile.Request](name, mgr, options)
}

// NewTypedUnmanaged returns a new typed controller without adding it to the manager.
//
// The name must be unique as it is used to identify the controller in metrics and logs.
func NewTypedUnmanaged[request mcreconcile.ClusterAware[request]](name string, mgr mcmanager.Manager, options controller.TypedOptions[request]) (TypedController[request], error) {
	c, err := controller.NewTypedUnmanaged[request](name, options)
	if err != nil {
		return nil, err
	}
	return &mcController[request]{
		TypedController: c,
		clusters:        make(map[string]*engagedCluster),
	}, nil
}

var _ TypedController[mcreconcile.Request] = &mcController[mcreconcile.Request]{}

type mcController[request mcreconcile.ClusterAware[request]] struct {
	controller.TypedController[request]

	lock     sync.Mutex
	clusters map[string]*engagedCluster
	sources  []mcsource.TypedSource[client.Object, request]
}

type engagedCluster struct {
	name    string
	cluster cluster.Cluster
	ctx     context.Context
	cancel  context.CancelFunc
}

func (c *mcController[request]) Engage(ctx context.Context, name string, cl cluster.Cluster) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	log.Printf("Engaging controller for cluster %q", name)

	// Check if we already have this cluster engaged with the SAME context
	if old, ok := c.clusters[name]; ok {
		if old.cluster == cl && old.ctx.Err() == nil {
			// Same impl, engagement still live → nothing to do
			log.Printf("Controller already engaged for cluster %q", name)
			return nil
		}
		// Re-engage: either old ctx is done, or impl changed. Stop the old one if still live.
		old.cancel()
		delete(c.clusters, name)
	}

	engCtx, cancel := context.WithCancel(ctx)

	// pass through in case the controller itself is cluster aware
	if ctrl, ok := c.TypedController.(multicluster.Aware); ok {
		log.Printf("Passing through engagement to cluster aware controller for cluster %q", name)
		if err := ctrl.Engage(engCtx, name, cl); err != nil {
			cancel()
			return err
		}
	}

	// engage cluster aware instances
	log.Printf("Engaging sources for controller for cluster %q", name)
	for _, aware := range c.sources {
		src, err := aware.ForCluster(name, cl)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to engage for cluster %q: %w", name, err)
		}
		if err := c.TypedController.Watch(startWithinContext[request](engCtx, src)); err != nil {
			cancel()
			return fmt.Errorf("failed to watch for cluster %q: %w", name, err)
		}
	}

	log.Printf("Controller successfully engaged for cluster %q", name)
	ec := &engagedCluster{
		name:    name,
		cluster: cl,
		ctx:     engCtx,
		cancel:  cancel,
	}
	c.clusters[name] = ec
	go func(token *engagedCluster) {
		log.Printf("Controller monitoring engagement for cluster %q", name)
		<-token.ctx.Done()
		c.lock.Lock()
		defer c.lock.Unlock()
		log.Printf("Controller disengaging for cluster %q", name)
		if cur, ok := c.clusters[token.name]; ok && cur == token {
			delete(c.clusters, token.name)
		}
		// note: cancel() is driven by parent; no need to call here
	}(ec)

	return nil
}

func (c *mcController[request]) MultiClusterWatch(src mcsource.TypedSource[client.Object, request]) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	for name, eng := range c.clusters {
		src, err := src.ForCluster(name, eng.cluster)
		if err != nil {
			return fmt.Errorf("failed to engage for cluster %q: %w", name, err)
		}
		if err := c.TypedController.Watch(startWithinContext[request](eng.ctx, src)); err != nil {
			return fmt.Errorf("failed to watch for cluster %q: %w", name, err)
		}
	}

	c.sources = append(c.sources, src)

	return nil
}

func startWithinContext[request mcreconcile.ClusterAware[request]](ctx context.Context, src source.TypedSource[request]) source.TypedSource[request] {
	return source.TypedFunc[request](func(ctlCtx context.Context, w workqueue.TypedRateLimitingInterface[request]) error {
		ctx, cancel := context.WithCancel(ctx)
		go func() {
			<-ctlCtx.Done()
			cancel()
		}()
		return src.Start(ctx, w)
	})
}

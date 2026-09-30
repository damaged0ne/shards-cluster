package azure

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/monitor/armmonitor"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/mysql/armmysqlflexibleservers"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/postgresql/armpostgresqlflexibleservers/v4"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/redis/armredis/v3"
)

// azureAPI is the part of the Azure APIs used by the integration (replaced by a fake in tests).
// The listings return all the pages; an empty resourceGroup means the whole subscription.
type azureAPI interface {
	listPostgres(subscription, resourceGroup string) ([]*armpostgresqlflexibleservers.Server, error)
	listMySQL(subscription, resourceGroup string) ([]*armmysqlflexibleservers.Server, error)
	listRedis(subscription, resourceGroup string) ([]*armredis.ResourceInfo, error)
	metrics(resourceID string, options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error)
}

type subscriptionClients struct {
	postgres *armpostgresqlflexibleservers.ServersClient
	mysql    *armmysqlflexibleservers.ServersClient
	redis    *armredis.Client
}

type sdkAPI struct {
	clients    map[string]*subscriptionClients
	monitor    *armmonitor.MetricsClient
	apiContext func() (context.Context, context.CancelFunc)
}

func newSDKAPI(cred azcore.TokenCredential, subscriptions []string, apiContext func() (context.Context, context.CancelFunc)) (*sdkAPI, error) {
	opts := &arm.ClientOptions{ClientOptions: policy.ClientOptions{
		Retry: policy.RetryOptions{MaxRetries: 3}, // bounded by apiContext anyway
	}}
	a := &sdkAPI{clients: map[string]*subscriptionClients{}, apiContext: apiContext}
	for _, s := range subscriptions {
		c := &subscriptionClients{}
		var err error
		if c.postgres, err = armpostgresqlflexibleservers.NewServersClient(s, cred, opts); err != nil {
			return nil, err
		}
		if c.mysql, err = armmysqlflexibleservers.NewServersClient(s, cred, opts); err != nil {
			return nil, err
		}
		if c.redis, err = armredis.NewClient(s, cred, opts); err != nil {
			return nil, err
		}
		a.clients[s] = c
	}
	var err error
	// the metrics are requested by resource URI: the subscription of the client doesn't matter
	if a.monitor, err = armmonitor.NewMetricsClient(subscriptions[0], cred, opts); err != nil {
		return nil, err
	}
	return a, nil
}

type pager[T any] interface {
	More() bool
	NextPage(context.Context) (T, error)
}

// all fetches all the pages, each one bounded by apiContext.
func all[P any, T any](a *sdkAPI, p pager[P], items func(P) []T) ([]T, error) {
	var res []T
	for p.More() {
		ctx, cancel := a.apiContext()
		page, err := p.NextPage(ctx)
		cancel()
		if err != nil {
			return res, err
		}
		res = append(res, items(page)...)
	}
	return res, nil
}

func (a *sdkAPI) listPostgres(subscription, resourceGroup string) ([]*armpostgresqlflexibleservers.Server, error) {
	c := a.clients[subscription].postgres
	if resourceGroup != "" {
		return all(a, c.NewListByResourceGroupPager(resourceGroup, nil), func(p armpostgresqlflexibleservers.ServersClientListByResourceGroupResponse) []*armpostgresqlflexibleservers.Server {
			return p.Value
		})
	}
	return all(a, c.NewListPager(nil), func(p armpostgresqlflexibleservers.ServersClientListResponse) []*armpostgresqlflexibleservers.Server {
		return p.Value
	})
}

func (a *sdkAPI) listMySQL(subscription, resourceGroup string) ([]*armmysqlflexibleservers.Server, error) {
	c := a.clients[subscription].mysql
	if resourceGroup != "" {
		return all(a, c.NewListByResourceGroupPager(resourceGroup, nil), func(p armmysqlflexibleservers.ServersClientListByResourceGroupResponse) []*armmysqlflexibleservers.Server {
			return p.Value
		})
	}
	return all(a, c.NewListPager(nil), func(p armmysqlflexibleservers.ServersClientListResponse) []*armmysqlflexibleservers.Server {
		return p.Value
	})
}

func (a *sdkAPI) listRedis(subscription, resourceGroup string) ([]*armredis.ResourceInfo, error) {
	c := a.clients[subscription].redis
	if resourceGroup != "" {
		return all(a, c.NewListByResourceGroupPager(resourceGroup, nil), func(p armredis.ClientListByResourceGroupResponse) []*armredis.ResourceInfo {
			return p.Value
		})
	}
	return all(a, c.NewListBySubscriptionPager(nil), func(p armredis.ClientListBySubscriptionResponse) []*armredis.ResourceInfo {
		return p.Value
	})
}

func (a *sdkAPI) metrics(resourceID string, options *armmonitor.MetricsClientListOptions) (armmonitor.MetricsClientListResponse, error) {
	ctx, cancel := a.apiContext()
	defer cancel()
	return a.monitor.List(ctx, resourceID, options)
}

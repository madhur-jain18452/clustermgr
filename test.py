from cluster_manager.global_cluster_cache import GlobalClusterCache
import json
import yaml

TEST_1 = False
TEST_2 = True

if __name__ == "__main__":
    config = None
    users = None
    with open('clusters.yml') as stream:
        try:
            config = yaml.safe_load(stream)
        except Exception as ex:
            raise ex
    with open('users.yaml') as stream:
        try:
            users = yaml.safe_load(stream)
        except Exception as ex:
            raise ex
    cluster_list = config['clusters']
    users_list = users['users']

    gcm = GlobalClusterCache(cluster_list, users_list, cache_clusters=False)
    gcm.summary()
    gcm.update_prefix('anjali.mishra@nutanix.com', 'ajay', op='add')
    gcm.summary()


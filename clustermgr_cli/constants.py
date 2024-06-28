
LOCAL_ENDPOINT = "http://127.0.0.1:5000/"
CLUSTER_EP = "clusters"
USER_EP = "users"
CACHE_EP = "cache"
TOOL_EP = "tool"


# We need application/json for CLI requests
# If this is not present in the request, the page will return an HTML page
CLI_HEADERS = {
  "Accept": "application/json"
}
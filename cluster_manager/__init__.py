"""The actual cache and its lock that will be maintained in the memory
Maps User email ID to their information
"""


"""Maps User prefixes to their email ID
Useful for quick mapping the VM to the user

I am planning to have buckets to speed up the process
    (No need to traverse all the available prefixes --
    just use the first letter to map)
For e.g.:
    For sahil, the prefixes are defined as 'sahil', 'sn' and 'ns'
    so we will add the prefix as following:
    's': {'sahil': 'sahil.naphade@nutanix.com', 'sn': 'sahil.naphade@nutanix.com'}
    and 
    'n': {'ns': sahil.naphade@nutanix.com'}
    as
    USER_PREFIX_EMAIL_MAP = {'s': {...}, 'n': {...}}

If prefix conflict occurs, we raise error and ask for a new prefix.

Uses the same lock as USER_CACHE for updating as both the caches are interdependent. 
"""

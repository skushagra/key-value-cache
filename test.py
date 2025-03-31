from sdk import CacheSDK

c = CacheSDK()

print(c.set("key", "value1"))
print(c.get("key"))
print(c.get("key1"))
#include <ts/ts.h>
#include <iostream>
#include <vector>
#include <string>
#include <fcntl.h>
#include <assert.h>
#include <string.h>

#include "credis.h"

using namespace std;

static int 
cache_read(INKCont contp, INKEvent event, void *edata)
{
	INKDebug("cache_plugin", "[cache_read]");
	
	REDIS redis_connection = (REDIS) INKContDataGet(contp);
	INKHttpTxn txnp = (INKHttpTxn) edata;

	void *key = 0;
	int keySize = 0;
	uint64_t size, offset;

	// get the key for the lookup
	INKCacheKeyGet(txnp, &key, &keySize);
	INKCacheBufferInfoGet(txnp, &size, &offset);
	  
	// lookup in cache and send the date to cache reenable
	const void *cacheData = 0;
	uint64_t cacheSize = 0;

	if (event == INK_EVENT_CACHE_LOOKUP) {
		event = INK_EVENT_CACHE_LOOKUP_COMPLETE;
	} else {
		event = INK_EVENT_CACHE_READ_COMPLETE;
	}	
	
	if (key != 0 && keySize > 0 && redis_connection) {
		std::string keyString((char *) key, keySize);
			
		//INKDebug("cache_plugin", "get value from redis w/ %d bytes key: %s", keySize, keyString.c_str());
		
		char *val = NULL;
		size_t val_len = 0;
		int rval = (size > 0) ? 
			credis_getv(redis_connection, (const char *) key, keySize, &val, &val_len) : 
			credis_existsv(redis_connection, (const char *) key, keySize);		
				
		if (rval >= 0) {
			size_t valSize = val_len;
				
			if (size > 0 && offset < valSize) {
				cacheData = val + offset;
				cacheSize = (size + offset > valSize) ? (valSize - offset) : size;
	
				if (cacheSize + offset < valSize) {
					if (event == INK_EVENT_CACHE_LOOKUP_COMPLETE) {
		            	event = INK_EVENT_CACHE_LOOKUP_READY;
		          	} else {
		            	event = INK_EVENT_CACHE_READ_READY;
		          	}
				}
			}
			
			INKDebug("cache_plugin", "cache hitted for key: %s w/ %d bytes value", keyString.c_str(), valSize);
		} else {
			INKDebug("cache_plugin", "cache missed for key: %s", keyString.c_str());
		}
	}
	
	return INKHttpCacheReenable(txnp, event, cacheData, cacheSize);
}

static int
cache_write(INKCont contp, INKEvent event, void *edata)
{
	INKDebug("cache_plugin", "[cache_write]");

	REDIS redis_connection = (REDIS) INKContDataGet(contp);
	INKHttpTxn txnp = (INKHttpTxn) edata;
	
	// get the key for the data
	void *key;
	int keySize;
	INKCacheKeyGet(txnp, &key, &keySize);
	
	uint64_t cacheSize = 0;
	
	if (key != 0 && keySize > 0 && redis_connection) {				
		// get the buffer to write into cache and get the start of the buffer
		INKIOBufferReader buffer = INKCacheBufferReaderGet(txnp);		
		INKIOBufferBlock block = INKIOBufferReaderStart(buffer);
		int available = INKIOBufferReaderAvail(buffer);				
		
		if (available > 0) {		
			char *buf = new char[available], *val = buf;
			
			do {
				int read;								
				const char *data = INKIOBufferBlockReadStart(block, buffer, &read);
				
				if (data != NULL) {					
					memcpy((void *) val, data, read);
					INKIOBufferReaderConsume(buffer, read);
					
					val += read;
				}
			} while ((block = INKIOBufferBlockNext(block)) != NULL);
			
			cacheSize = val - buf;
			
			int rval = credis_appendv(redis_connection, (const char *) key, keySize, buf, cacheSize);
			
			if (rval >= (int) cacheSize)  {
				INKDebug("cache_plugin", "put %d bytes value to redis w/ %d bytes key: %p", available, keySize, key);
			} else {
				INKDebug("cache_plugin", "fail to put value to redis, %d", rval);				
			}
		}
	}
	
	return INKHttpCacheReenable(txnp, event, 0, cacheSize);
}

static int
cache_remove(INKCont contp, INKEvent event, void *edata)
{  
	INKDebug("cache_plugin", "[cache_remove]");
  
	REDIS redis_connection = (REDIS) INKContDataGet(contp);
	INKHttpTxn txnp = (INKHttpTxn) edata;
	
	// get the key for the data
	void *key;
	int keySize;
	INKCacheKeyGet(txnp, &key, &keySize);	
	
	if (key != 0 && keySize > 0 && redis_connection) {
		int rval = credis_delv(redis_connection, (const char *) key, keySize);
		
		if (CREDIS_OK == rval) {
			INKDebug("cache_plugin", "remove item from redis w/ %d bytes key: %p", keySize, key);
		} else {
			INKDebug("cache_plugin", "fail to remove item from redis, %d", rval);
		}		
	}
  
	return INKHttpCacheReenable(txnp, event, 0, 0);
}

static int 
redis_cache(INKCont contp, INKEvent event, void *edata)
{
	INKHttpTxn txnp = (INKHttpTxn) edata;
	
  	switch (event) {
		// read events
		case INK_EVENT_CACHE_LOOKUP:
		case INK_EVENT_CACHE_READ:
    		return cache_read(contp, event, edata);
    		break;

    	// write events
  		case INK_EVENT_CACHE_WRITE:
  		case INK_EVENT_CACHE_WRITE_HEADER:
    		return cache_write(contp, event, edata);
    		break;

    	// delete events
  		case INK_EVENT_CACHE_DELETE:
    		return cache_remove(contp, event, edata);
    		break;

  		case INK_EVENT_CACHE_CLOSE:
    		return INKHttpCacheReenable(txnp, event, 0, 0);
    		break;

  		default:
    		INKDebug("cache_plugin", "ERROR: unknown event");
    }
    	
	return 0;
}

void 
INKPluginInit(const int argc, const char **argv)
{
	INKPluginRegistrationInfo info;
	
	INKDebug("cache_plugin", "[INKPluginInit] Starting redis cache plugin");
	
  	info.plugin_name = (char *) "redis_cache";
  	info.vendor_name = (char *) "Flier Lu";
  	info.support_email = (char *) "flier.lu@gmail.com";
  	
  	REDIS redis_connection = credis_connect(NULL, 0, 10000);
  	
  	if (!redis_connection)
  	{
  		INKDebug("cache_plugin", "fail to connect redis server");
  		return;
  	}
  	
  	int rval = credis_ping(redis_connection);
  	
  	if (CREDIS_OK != rval)
  	{
  		INKDebug("cache_plugin", "fail to ping redis server, %d", rval);
  		credis_close(redis_connection);
  		return;
  	}
	
	INKCont continuation_plugin = INKContCreate(redis_cache, INKMutexCreate());
	
	INKContDataSet(continuation_plugin, redis_connection);
	
	INKReturnCode ival = INKCacheHookAdd(INK_CACHE_PLUGIN_HOOK, continuation_plugin);
	
	if (INK_SUCCESS != ival)
		INKError("fail to add cache hook, %d", ival);
}
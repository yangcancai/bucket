#include "../redismodule.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include <string.h>
const int MAX = 20;
RedisModuleString * CreateId(RedisModuleCtx *ctx, RedisModuleString *bucket_key, RedisModuleString *id) {
	return RedisModule_CreateStringPrintf(ctx, "%s:%s", RedisModule_StringPtrLen(bucket_key, NULL), RedisModule_StringPtrLen(id, NULL));
}
void ReleaseId(RedisModuleCtx *ctx, RedisModuleString *id){
	RedisModule_FreeString(ctx, id);
}
/* BUCKET.INSERT  bucket_key id  expired score name field name field ...
 * command. */
int BucketInsert_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc < 7 || (argc >= 7 && argc % 2 == 0) || argc > (MAX * 2 + 5)) return RedisModule_WrongArity(ctx);
    RedisModuleString *id_str = CreateId(ctx, argv[1], argv[2]);
    RedisModuleKey *id = RedisModule_OpenKey(ctx, id_str, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(id)  != REDISMODULE_KEYTYPE_HASH &&
	RedisModule_KeyType(id) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    // bucket_key be a zset
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
   if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET&&
	RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(key);
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);

   }
    long long expired;
    double score;
    if ((RedisModule_StringToDouble(argv[4],&score) != REDISMODULE_OK) ||
        (score < 0) || score > 4503599627370496) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,"ERR invalid score, must less than 4503599627370497");
    }
 
    if ((RedisModule_StringToLongLong(argv[3],&expired) != REDISMODULE_OK) ||
        (expired < 0)) {
        RedisModule_CloseKey(key);
        RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,"ERR invalid expred");
    }
    // zset add id to bucket_key
   RedisModule_ZsetAdd(key, score, id_str, NULL); 
    // hashset id
   for (int i = 5; i < argc; i += 2 ){
    	RedisModule_HashSet(id,REDISMODULE_HASH_NONE,argv[i],argv[i+1],NULL);
   }
    RedisModuleString *str_id = RedisModule_CreateString(ctx, "id", 2);
    RedisModule_HashSet(id, REDISMODULE_HASH_NONE, str_id, argv[2],NULL);
    RedisModule_FreeString(ctx, str_id);

    RedisModule_SetExpire(id, expired * 1000);
    RedisModule_CloseKey(key);
    RedisModule_CloseKey(id);
    ReleaseId(ctx, id_str);
    RedisModule_ReplyWithLongLong(ctx,1);
    return REDISMODULE_OK;
}
/*
* BUCKET.UPDATE bucket_key id name field
*
*/
int BucketUpdate_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
   if (argc < 5 || (argc >= 5 && argc % 2 == 0)) return RedisModule_WrongArity(ctx);
   RedisModuleString *id_str = CreateId(ctx, argv[1], argv[2]);
    RedisModuleKey *id = RedisModule_OpenKey(ctx, id_str, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(id)  != REDISMODULE_KEYTYPE_HASH &&
	RedisModule_KeyType(id) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    // bucket_key be a zset
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
   if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET&&
	RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(key);
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);

   }
   long existed = 0;
   for (int i = 3; i < argc; i += 2){
	RedisModuleString *field;
	RedisModule_HashGet(id, REDISMODULE_HASH_NONE, argv[i], &field, NULL);
	if (field){
		existed = 1;
		break;
	}
 
   }
   if (0 == existed){
	RedisModule_CloseKey(key);
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,"ERR id not existed");
   }
  // hashset id
   for (int i = 3; i < argc; i += 2 ){
    	RedisModule_HashSet(id,REDISMODULE_HASH_NONE,argv[i],argv[i+1],NULL);
   }
    RedisModule_CloseKey(key);
    RedisModule_CloseKey(id);
    ReleaseId(ctx, id_str);
    RedisModule_ReplyWithLongLong(ctx,1);
    return REDISMODULE_OK;
}
/*
* BUCKET.INCR bucket_key id name counter
*/
int BucketIncr_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
   if (argc != 5) return RedisModule_WrongArity(ctx);
   RedisModuleString *id_str = CreateId(ctx, argv[1], argv[2]);
    RedisModuleKey *id = RedisModule_OpenKey(ctx,id_str, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(id)  != REDISMODULE_KEYTYPE_HASH &&
	RedisModule_KeyType(id) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    // bucket_key be a zset
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
   if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET&&
	RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(key);
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);

   }
   long existed = 0;
   RedisModuleString *field;
   RedisModule_HashGet(id, REDISMODULE_HASH_NONE, argv[3], &field, NULL);
  if (field){
		existed = 1;
   }
   if (0 == existed){
	RedisModule_CloseKey(key);
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,"ERR field not existed");
   }
    RedisModuleCallReply *reply;
    reply = RedisModule_Call(ctx,"HINCRBY","sss", id_str, argv[3], argv[4]);
    RedisModule_ReplyWithCallReply(ctx, reply);
    RedisModule_FreeCallReply(reply);
    RedisModule_CloseKey(id);
    RedisModule_CloseKey(key);
    ReleaseId(ctx, id_str);
    return REDISMODULE_OK;
}
int BucketDelAll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
   if (argc != 2) {
       return RedisModule_WrongArity(ctx);
   }   
    // bucket_key be a zset
   RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
   if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET&&
	RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);

   }
   // zset
    RedisModule_ZsetFirstInScoreRange(key,REDISMODULE_NEGATIVE_INFINITE,REDISMODULE_POSITIVE_INFINITE,0,0);
    while(!RedisModule_ZsetRangeEndReached(key)) {
    double score;
    RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key,&score);
	RedisModuleCallReply *reply;
	reply = RedisModule_Call(ctx,"DEL","s",ele);
    RedisModule_FreeCallReply(reply);
    RedisModule_FreeString(ctx, ele);
    RedisModule_ZsetRangeNext(key);
    }
    RedisModule_ZsetRangeStop(key);
    RedisModuleCallReply *reply1;
	reply1 = RedisModule_Call(ctx,"DEL","s",argv[1]);
    RedisModule_ReplyWithCallReply(ctx, reply1);
 	RedisModule_FreeCallReply(reply1);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;

}

/*
* BUCKET.DEL bucket_key
* BUCKET.DEL bucket_key id
* BUCKET.DEL bucket_key id name
*/
int BucketDel_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
   if (argc == 2) {
       return BucketDelAll_RedisCommand(ctx, argv, argc);
   }else if(argc != 3 && argc != 4){
       return RedisModule_WrongArity(ctx);
   }   
   RedisModuleString *id_str = CreateId(ctx, argv[1], argv[2]);
    RedisModuleKey *id = RedisModule_OpenKey(ctx,id_str, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(id)  != REDISMODULE_KEYTYPE_HASH &&
	RedisModule_KeyType(id) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    // bucket_key be a zset
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
   if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET&&
	RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(key);
	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);

   }
   if (argc == 4){
    RedisModuleCallReply *reply;
    reply = RedisModule_Call(ctx,"HDEL","ss", id_str, argv[3]);
    RedisModule_ReplyWithCallReply(ctx, reply);
    RedisModule_FreeCallReply(reply);
   }else{
    RedisModuleCallReply *reply;
    reply = RedisModule_Call(ctx,"DEL","s", id_str);
    RedisModule_ZsetRem(key, id_str, NULL);
    RedisModule_ReplyWithCallReply(ctx, reply);
    RedisModule_FreeCallReply(reply);
   }
    RedisModule_CloseKey(id);
    RedisModule_CloseKey(key);
    ReleaseId(ctx, id_str);
    return REDISMODULE_OK;
}
 
/* 
* BUCKET.GETALL bucket_key
*/
int BucketGetAll_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
    if (argc != 2) return RedisModule_WrongArity(ctx);
 // bucket_key be a zset
    RedisModuleKey *key = RedisModule_OpenKey(ctx,argv[1],
        REDISMODULE_READ|REDISMODULE_WRITE);
   if (RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_ZSET&&
	RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);

   }
   // zsetall 
    RedisModule_ZsetFirstInScoreRange(key,REDISMODULE_NEGATIVE_INFINITE,REDISMODULE_POSITIVE_INFINITE,0,0);
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    int arraylen = 0;
    RedisModuleCallReply *reply_count;
    reply_count = RedisModule_Call(ctx,"ZCARD","s",argv[1]);
    size_t bucket_len = RedisModule_CallReplyInteger(reply_count);
    RedisModule_FreeCallReply(reply_count);
    RedisModuleString** remove_list = RedisModule_Alloc(bucket_len * sizeof(RedisModuleString*));
    size_t i = 0;
    while(!RedisModule_ZsetRangeEndReached(key)) {
        double score;
        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key,&score);
	RedisModuleCallReply *reply;
	reply = RedisModule_Call(ctx,"HGETALL","s",ele);
	size_t reply_len = RedisModule_CallReplyLength(reply);
	if (reply_len > 0){
        	RedisModule_ReplyWithCallReply(ctx,reply);
        	arraylen += 1;
        	RedisModule_FreeString(ctx,ele);
		remove_list[i++] = NULL;
	}else{
		remove_list[i++] = ele;
	}
 	RedisModule_FreeCallReply(reply);
        RedisModule_ZsetRangeNext(key);
    }
    RedisModule_ZsetRangeStop(key);
    for (i = 0; i < bucket_len; i++){
	    if (remove_list[i] != NULL){
		// const char *s = RedisModule_StringPtrLen(remove_list[i],NULL);
    		// RedisModule_Log(ctx,"warning","%zu is %s",i, s);
		RedisModule_ZsetRem(key, remove_list[i], NULL);
        	RedisModule_FreeString(ctx, remove_list[i]);
	    }else{
    		// RedisModule_Log(ctx,"warning","%zu is null",i);
	    }
    }
    RedisModule_Free(remove_list);
    RedisModule_ReplySetArrayLength(ctx,arraylen);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}

/*
* BUCKET.GET bucket_key
* BUCKET.GET bucket_key id
* BUCKET.GET bucket_key id name
*/
int BucketGet_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
   if (argc == 2) {
	   return BucketGetAll_RedisCommand(ctx, argv, argc);
   }else if (argc < 3){
	   return  RedisModule_WrongArity(ctx);
   }
    RedisModuleString *id_str = CreateId(ctx, argv[1], argv[2]);
    RedisModuleKey *id = RedisModule_OpenKey(ctx, id_str, REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(id)  != REDISMODULE_KEYTYPE_HASH &&
	RedisModule_KeyType(id) != REDISMODULE_KEYTYPE_EMPTY){
		RedisModule_CloseKey(id);
		ReleaseId(ctx, id_str);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
   // getid
   if (argc == 3){
   	RedisModuleCallReply *reply;
    	reply = RedisModule_Call(ctx,"HGETALL","s",id_str);
	RedisModule_ReplyWithCallReply(ctx,reply);
        RedisModule_FreeCallReply(reply);
    	RedisModule_CloseKey(id);
	ReleaseId(ctx, id_str);
        return REDISMODULE_OK;
   }
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    long long arraylen = 0;
   for (int i = 3; i < argc; i += 2){
	RedisModuleString *field;
	RedisModule_HashGet(id, REDISMODULE_HASH_NONE, argv[i], &field, NULL);
	if (field){
		arraylen += 2;
   		RedisModule_ReplyWithString(ctx,argv[i]);
   		RedisModule_ReplyWithString(ctx,field);
	}
   }
    RedisModule_ReplySetArrayLength(ctx,arraylen);
    RedisModule_CloseKey(id);
    ReleaseId(ctx, id_str);
    return REDISMODULE_OK;
}
/*
* BUCKET.Range bucket_key start_score end_score
*/
int BucketRange_RedisCommand(RedisModuleCtx *ctx, RedisModuleString **argv, int argc)
{
   if (argc != 4){
	   return  RedisModule_WrongArity(ctx);
   }
    RedisModuleKey *key = RedisModule_OpenKey(ctx, argv[1], REDISMODULE_READ|REDISMODULE_WRITE);
    if (RedisModule_KeyType(key)  != REDISMODULE_KEYTYPE_ZSET&&
	RedisModule_KeyType(key) != REDISMODULE_KEYTYPE_EMPTY){
	RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    long double start_score;
    long double end_score;
    if (RedisModule_StringToLongDouble(argv[2], &start_score) != REDISMODULE_OK){
	RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    if (RedisModule_StringToLongDouble(argv[3], &end_score) != REDISMODULE_OK){
	RedisModule_CloseKey(key);
        return RedisModule_ReplyWithError(ctx,REDISMODULE_ERRORMSG_WRONGTYPE);
    }
    RedisModuleCallReply *reply_count;
    reply_count = RedisModule_Call(ctx,"ZCOUNT","sss",argv[1], argv[2], argv[3]);
    size_t bucket_len = RedisModule_CallReplyInteger(reply_count);
    RedisModule_FreeCallReply(reply_count);
    RedisModuleString** remove_list = RedisModule_Alloc(bucket_len * sizeof(RedisModuleString*));
    size_t i = 0;
    size_t arraylen = 0;
    RedisModule_ZsetFirstInScoreRange(key,start_score,end_score,0,0);
    RedisModule_ReplyWithArray(ctx,REDISMODULE_POSTPONED_ARRAY_LEN);
    while(!RedisModule_ZsetRangeEndReached(key)) {
        double score;
        RedisModuleString *ele = RedisModule_ZsetRangeCurrentElement(key,&score);
	RedisModuleCallReply *reply;
	reply = RedisModule_Call(ctx,"HGETALL","s",ele);
	size_t reply_len = RedisModule_CallReplyLength(reply);
	if (reply_len > 0){
        	RedisModule_ReplyWithCallReply(ctx,reply);
        	arraylen += 1;
        	RedisModule_FreeString(ctx,ele);
		remove_list[i++] = NULL;
	}else{
		remove_list[i++] = ele;
	}
 	RedisModule_FreeCallReply(reply);
        RedisModule_ZsetRangeNext(key);
    }
    RedisModule_ZsetRangeStop(key);
    for (i = 0; i < bucket_len; i++){
	    if (remove_list[i] != NULL){
		const char *s = RedisModule_StringPtrLen(remove_list[i],NULL);
    		RedisModule_Log(ctx,"warning","%zu is %s",i, s);
		RedisModule_ZsetRem(key, remove_list[i], NULL);
        	RedisModule_FreeString(ctx, remove_list[i]);
	    }else{
    		RedisModule_Log(ctx,"warning","%zu is null",i);
	    }
    }
    RedisModule_Free(remove_list);
    RedisModule_ReplySetArrayLength(ctx,arraylen);
    RedisModule_CloseKey(key);
    return REDISMODULE_OK;
}
/* This function must be present on each Redis module. It is used in order to
 * register the commands into the Redis server. */
int RedisModule_OnLoad(RedisModuleCtx *ctx, RedisModuleString **argv, int argc) {
    if (RedisModule_Init(ctx,"bucket",1,REDISMODULE_APIVER_1)
        == REDISMODULE_ERR) return REDISMODULE_ERR;

    /* Log the list of parameters passing loading the module. */
    for (int j = 0; j < argc; j++) {
        const char *s = RedisModule_StringPtrLen(argv[j],NULL);
        RedisModule_Log(ctx, "notice", "Module loaded with ARGV[%d] = %s\n", j, s);
    }
    if (RedisModule_CreateCommand(ctx,"bucket.insert",
        BucketInsert_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"bucket.update",
        BucketUpdate_RedisCommand,"write deny-oom",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

    if (RedisModule_CreateCommand(ctx,"bucket.get",
        BucketGet_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
	
   if (RedisModule_CreateCommand(ctx,"bucket.getall",
        BucketGetAll_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;
   if (RedisModule_CreateCommand(ctx,"bucket.range",
        BucketRange_RedisCommand,"readonly",1,1,1) == REDISMODULE_ERR)
        return REDISMODULE_ERR;

   if (RedisModule_CreateCommand(ctx,"bucket.incr",
         BucketIncr_RedisCommand,"write deny-oom",1,2,1) == REDISMODULE_ERR)
         return REDISMODULE_ERR;

   if (RedisModule_CreateCommand(ctx,"bucket.del",
         BucketDel_RedisCommand,"write deny-oom",1,2,1) == REDISMODULE_ERR)
         return REDISMODULE_ERR;

     return REDISMODULE_OK;
}

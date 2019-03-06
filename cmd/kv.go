package cmd

/*
#include <stdio.h>
#include <stdlib.h>
#include "nkv_api.h"
#include "nkv_result.h"

struct minio_nkv_handle {
  uint64_t nkv_handle;
  uint64_t container_hash;
  uint64_t network_path_hash;
};

static int minio_nkv_open(char *config, uint64_t *nkv_handle) {
  uint64_t instance_uuid = 0;
  nkv_result result;
  result = nkv_open(config, "minio", "msl-ssg-sk01", 1023, &instance_uuid, nkv_handle);
  return result;
}

static int minio_nkv_open_path(struct minio_nkv_handle *handle, char *ipaddr) {
  uint32_t index = 0;
  uint32_t cnt_count = NKV_MAX_ENTRIES_PER_CALL;
  nkv_container_info *cntlist = malloc(sizeof(nkv_container_info)*NKV_MAX_ENTRIES_PER_CALL);
  memset(cntlist, 0, sizeof(nkv_container_info) * NKV_MAX_ENTRIES_PER_CALL);

  for (int i = 0; i < NKV_MAX_ENTRIES_PER_CALL; i++) {
    cntlist[i].num_container_transport = NKV_MAX_CONT_TRANSPORT;
    cntlist[i].transport_list = malloc(sizeof(nkv_container_transport)*NKV_MAX_CONT_TRANSPORT);
    memset(cntlist[i].transport_list, 0, sizeof(nkv_container_transport)*NKV_MAX_CONT_TRANSPORT);
  }

  int result = nkv_physical_container_list (handle->nkv_handle, index, cntlist, &cnt_count);
  if (result != 0) {
    printf("NKV getting physical container list failed !!, error = %d\n", result);
    exit(1);
  }

  nkv_io_context io_ctx[16];
  memset(io_ctx, 0, sizeof(nkv_io_context) * 16);
  uint32_t io_ctx_cnt = 0;

  for (uint32_t i = 0; i < cnt_count; i++) {
    io_ctx[io_ctx_cnt].container_hash = cntlist[i].container_hash;

    for (int p = 0; p < cntlist[i].num_container_transport; p++) {
      printf("Transport information :: hash = %u, id = %d, address = %s, port = %d, family = %d, speed = %d, status = %d, numa_node = %d\n",
              cntlist[i].transport_list[p].network_path_hash, cntlist[i].transport_list[p].network_path_id, cntlist[i].transport_list[p].ip_addr,
              cntlist[i].transport_list[p].port, cntlist[i].transport_list[p].addr_family, cntlist[i].transport_list[p].speed,
              cntlist[i].transport_list[p].status, cntlist[i].transport_list[p].numa_node);
      io_ctx[io_ctx_cnt].is_pass_through = 1;
      io_ctx[io_ctx_cnt].container_hash = cntlist[i].container_hash;
      io_ctx[io_ctx_cnt].network_path_hash = cntlist[i].transport_list[p].network_path_hash;
      if(!strcmp(cntlist[i].transport_list[p].ip_addr, ipaddr)) {
              handle->container_hash = cntlist[i].container_hash;
              handle->network_path_hash = cntlist[i].transport_list[p].network_path_hash;
              return 0;
      }
      io_ctx_cnt++;
    }
  }
  return 1;
}

static int minio_nkv_put(struct minio_nkv_handle *handle, void *key, int keyLen, void *value, int valueLen) {
  nkv_result result;
  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  nkv_store_option option = {0};
  nkv_value nkvvalue = {value, valueLen, 0};
  result = nkv_store_kvp(handle->nkv_handle, &ctx, &nkvkey, &option, &nkvvalue);
  return result;
}

static int minio_nkv_get(struct minio_nkv_handle *handle, void *key, int keyLen, void *value, int valueLen, int *actual_length) {
  nkv_result result;
  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  nkv_retrieve_option option = {0};

  nkv_value nkvvalue = {value, valueLen, 0};
  result = nkv_retrieve_kvp(handle->nkv_handle, &ctx, &nkvkey, &option, &nkvvalue);
  *actual_length = nkvvalue.actual_length;
  return result;
}

static int minio_nkv_delete(struct minio_nkv_handle *handle, void *key, int keyLen) {
  nkv_result result;
  nkv_io_context ctx;
  ctx.is_pass_through = 1;
  ctx.container_hash = handle->container_hash;
  ctx.network_path_hash = handle->network_path_hash;
  ctx.ks_id = 0;

  const nkv_key  nkvkey = {key, keyLen};
  result = nkv_delete_kvp(handle->nkv_handle, &ctx, &nkvkey);
  return result;
}

*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"unsafe"
)

var globalNKVHandle C.uint64_t

func minio_nkv_open(configPath string) error {
	if globalNKVHandle != 0 {
		return nil
	}
	status := C.minio_nkv_open(C.CString(configPath), &globalNKVHandle)
	if status != 0 {
		return errDiskNotFound
	}
	return nil
}

func newKVSync(path string) (*KVSync, error) {
	kv := &KVSync{}
	kv.path = path
	kv.handle.nkv_handle = globalNKVHandle
	status := C.minio_nkv_open_path(&kv.handle, C.CString(path))
	if status != 0 {
		fmt.Println("unable to open", path, status)
		return nil, errors.New("unable to open device")
	}
	return kv, nil
}

type KVSync struct {
	handle C.struct_minio_nkv_handle
	path   string
}

var kvValuePool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, kvMaxValueSize)
		return &b
	},
}

const kvKeyLength = 200

func (k *KVSync) Put(keyStr string, value []byte) error {
	if len(value) > kvMaxValueSize {
		return errValueTooLong
	}
	key := []byte(keyStr)
	for len(key) < kvKeyLength {
		key = append(key, '\x00')
	}
	if len(key) > kvKeyLength {
		fmt.Println("invalid key length", key, len(key))
		os.Exit(0)
	}
	var valuePtr unsafe.Pointer
	if len(value) > 0 {
		valuePtr = unsafe.Pointer(&value[0])
	}
	status := C.minio_nkv_put(&k.handle, unsafe.Pointer(&key[0]), C.int(len(key)), valuePtr, C.int(len(value)))
	if status != 0 {
		return errors.New("error during put")
	}
	return nil
}

func (k *KVSync) Get(keyStr string, value []byte) ([]byte, error) {
	key := []byte(keyStr)
	for len(key) < kvKeyLength {
		key = append(key, '\x00')
	}
	if len(key) > kvKeyLength {
		fmt.Println("invalid key length", key, len(key))
		os.Exit(0)
	}
	var actualLength C.int

	tries := 10
	for {
		status := C.minio_nkv_get(&k.handle, unsafe.Pointer(&key[0]), C.int(len(key)), unsafe.Pointer(&value[0]), C.int(len(value)), &actualLength)
		if status != 0 {
			return nil, errFileNotFound
		}
		if actualLength > 0 {
			break
		}
		tries--
		if tries == 0 {
			fmt.Println("GET failed (after 10 retries) on (actual_length=0)", k.path, keyStr)
			os.Exit(1)
		}
	}
	return value[:actualLength], nil
}

func (k *KVSync) Delete(keyStr string) error {
	key := []byte(keyStr)
	for len(key) < kvKeyLength {
		key = append(key, '\x00')
	}
	if len(key) > kvKeyLength {
		fmt.Println("invalid key length", key, len(key))
		os.Exit(0)
	}
	status := C.minio_nkv_delete(&k.handle, unsafe.Pointer(&key[0]), C.int(len(key)))
	if status != 0 {
		return errFileNotFound
	}
	return nil
}

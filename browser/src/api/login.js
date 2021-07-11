import request from '../utils/request';
import QS from 'qs';
import Vue from 'vue'
const basetConfig = Vue.prototype.data_api
// 用户登录
export function login(data) {
  return request({
    url: `/auth/login`,
    method: 'POST',
    data
  })
}


// 注销
export function logout(data) {
  return request({
    url: `/auth/logout`,
    method: 'POST',
    data:QS.stringify(data)
  })
}

export function webrpc(data) {
  return request({
    url: `/minio/webrpc`,
    method: 'POST',
    data
  })
}


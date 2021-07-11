import axios from 'axios' // 引入axios  npm install axios
import QS from 'qs'
import store from '../store'
import router from '../router'
import { Message } from 'element-ui'


const service = axios.create({
	baseURL: process.env.NODE_ENV === 'production' ? process.env.BASE_API : '/api', 
	timeout: 15000
})
//请求
service.interceptors.request.use(function (config) {
    if (sessionStorage.oaxLoginUserId) {
      config.baseURL === process.env.BASE_API
    }
    if (config.method === 'post') {
      // config.data = qs.stringify(config.data)
      // config.content-type = 'application/x-www-form-urlencoded'
    }
		config.headers['Authorization'] = "Bearer "+ store.getters.accessToken;
    sessionStorage.time = 70
    return config
}, function (error) {
	// Do something with request error
    console.log(error) // for debug
	return Promise.reject(error);
})
//响应
service.interceptors.response.use(response => {
	const res = response
    if (!res.data.success) {
      // -1:用户未登录;
      if (res.data.code === '-1') {
        store.dispatch('FedLogOut').then(() => {
          // location.reload() // 为了重新实例化vue-router对象 避免bug
		//   location.href = '/login'
			router.push('/login')
        })
      }
      return response.data
    } else {
      return response.data
	}
	return response.data
}, function (error) {
	// 失败处理
	console.log('responseError:' + error) // for debug
	Message({
		message: 'Error',
		type: 'error',
		duration: 5 * 1000
	  })
	/*switch (error.response.status) {
		case 400:
			error.message = '错误请求';
			break;
		case 401:
			error.message = '未授权，请重新登录';
			break;
		case 403:
			error.message = '拒绝访问';
			break;
		case 404:
			error.message = '请求错误,未找到该资源';
			//TODO 去访问404 页面
			break;
		case 405:
			error.message = '请求方法未允许';
			break;
		case 408:
			error.message = '请求超时';
			break;
		case 500:
			error.message = '服务器端出错';
			break;
		case 501:
			error.message = '网络未实现';
			break;
		case 502:
			error.message = '网络错误';
			break;
		case 503:
			error.message = '服务不可用';
			break;
		case 504:
			error.message = '网络超时';
			break;
		case 505:
			error.message = 'http版本不支持该请求';
			break;
		default:
			error.message = '网络连接错误';
	}*/
	return Promise.reject(error);
});

export default service;

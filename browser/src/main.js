// The Vue build version to load with the `import` command
// (runtime-only or standalone) has been set in webpack.base.conf with an alias.
import Vue from 'vue'
import App from './App'
import router from './router'
import Router from 'vue-router'
import VueClipboard from 'vue-clipboard2'
// import EventBus from '@/libs/EventBus';
// import $ from 'jquery'
import Vuex from 'vuex'
import store from './store'
import i18n from './lang' // 语言包

import ElementUI from 'element-ui';
import locale from 'element-ui/lib/locale/lang/en'
import 'element-ui/lib/theme-chalk/index.css'; // 默认主题
// import './assets/css/theme-green/index.css'; // 浅绿色主题
import './assets/css/icon.css';

// import { Button, Input, Pagination, Table, TableColumn, MessageBox } from 'element-ui';

import 'element-ui/lib/theme-chalk/index.css';

import * as filters from './filters'
import '@/permission' // 登陆控制

Vue.config.productionTip = false
Vue.prototype.data_api=process.env.NODE_ENV === 'production' ? `${window.location.protocol}//${window.location.host}` : 'https://85109788-c976-4e21-824a-018ef4517631.cygnus.filswan.com';

Vue.use(Router);
Vue.use(ElementUI, { locale })
// Vue.use(ElementUI);
Vue.use(Vuex)
Vue.use(VueClipboard)

Object.keys(filters).forEach(key => {
  Vue.filter(key, filters[key])
})

Vue.config.productionTip = false
Vue.prototype.NumberMul = function(arg1, arg2) {
  var m = 0;
  var s1 = arg1.toString();
  var s2 = arg2.toString();
  try {
      m += s1.split(".")[1].length;
  } catch (e) {}
  try {
      m += s2.split(".")[1].length;
  } catch (e) {}

  return Number(s1.replace(".", "")) * Number(s2.replace(".", "")) / Math.pow(10, m);
}
router.beforeEach((to, from, next) => {
  if (to.matched.some(record => record.meta.requiresAuth)) {
      //这里判断用户是否登录，验证本地存储是否有token
      if (!localStorage.token) { // 判断当前的token是否存在
          next({
              path: '/minio/login',
              query: { redirect: to.fullPath }
          })
      } else {
          next()
      }
  } else {
      next() // 确保一定要调用 next()
  }
  
  window.scrollTo(0,0);
})


/* eslint-disable no-new */
new Vue({
  el: '#app',
  components: { App },
  template: '<App/>',
  router,
  store,
  i18n
})

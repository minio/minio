import router from './router'
import NProgress from 'nprogress' // Progress 进度条
import 'nprogress/nprogress.css' // Progress 进度条样式
import store from './store'

NProgress.configure({
  showSpinner: false
})

const whiteList = ['/minio/login'] // 不重定向白名单
router.beforeEach((to, from, next) => {
  NProgress.start()
  store.state.user.linkPageName = null
  store.state.user.linkPageName = to.name
  if (sessionStorage.oaxLoginUserId) {
    if (to.path === '/minio/login') {
      next({
        path: '/'
      })
      NProgress.done()
    } else {
      next()
      NProgress.done()
    }
  } else {
    if (whiteList.indexOf(to.path) !== -1) { // 在免登录白名单，直接进入
      next()
    } else {
      next()
      NProgress.done()
    }
  }
})

router.afterEach(() => {
  NProgress.done() // 结束Progress
})

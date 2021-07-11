import Vue from 'vue'
import Router from 'vue-router'
Vue.use(Router)
//路由懒加载
const home = () => import("@/components/Home");
const minio = () => import("@/views/minio/index"); 
const login = () => import("@/components/login"); 


//配置路由
export default new Router({
	// mode: 'history', // 后端支持可开
	mode: 'hash',
	routes: [
        {
            path: '/',
            redirect: '/minio'
        },
        {
            path: '/',
            component: home,
            children: [
                {
                    path: '/minio',
                    name: 'minio',
                    component: minio,
					beforeEnter: (to, from, next) => {
						
						// 这里判断用户是否登录，验证本地存储是否有token
						if (!sessionStorage.getItem('oaxMinioLoginAccessToken')) { // 判断当前的token是否存在
							next({
								path: '/minio/login',
								query: { redirect: to.fullPath }
							})
						} else {
							next()
						}
				
					}
                },
            ]
        },
		//登录
		{
			path: '/minio/login',
			name: 'login',
			component: login,
		},
        {
            path: '*',
            redirect: '/'
        }
	]
})
const originalPush = Router.prototype.push
	Router.prototype.push = function push(location) {
    return originalPush.call(this, location).catch(err => err)
}
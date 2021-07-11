import { login, logout } from '@/api/login'
import { Message } from 'element-ui'

const user = {
  state: {
    name: sessionStorage.oaxLoginName || '',
    userId: sessionStorage.oaxLoginUserId || '',
    accessToken: sessionStorage.oaxMinioLoginAccessToken || ''
  },

  mutations: {
    SET_NAME: (state, name) => {
      state.name = name
    },
    SET_USERID: (state, userId) => {
      state.userId = userId
    },
    SET_ACCESSTOKEN: (state, accessToken) => {
      state.accessToken = accessToken
    }
  },

  actions: {
    // 登录
    Login({ commit }, userInfo) {
      var _this = this
      return new Promise((resolve, reject) => {
        login(userInfo)
          .then(response => {
            if (response.code === '10020') {
              Message({
                message: response.msg,
                type: 'error',
                duration: 5 * 1000
              })
              sessionStorage.oaxRegisterMail = userInfo.username
              _this.$router.push("/asset_management")
              return false
            }
            _this.loginLoad = false
            if (response.success === true) {
              sessionStorage.oaxLoginUserId = response.data.userId
              sessionStorage.oaxMinioLoginAccessToken = response.data.accessToken
              sessionStorage.oaxLoginName = response.data.name
              sessionStorage.oaxShowName = response.data.showName
              sessionStorage.oaxLoginEmail = response.data.email
              sessionStorage.oaxLoginPhone = response.data.phone
              sessionStorage.oaxLoginCheckStatus = response.data.checkStatus
              sessionStorage.oaxLoginLevel = response.data.level
              sessionStorage.oaxLoginRegisterTypee = response.data.registerType
              sessionStorage.oaxLoginSource = response.data.source
              const data = response.data
              commit('SET_NAME', data.name)
              commit('SET_SHOWNAME', data.showName)
              commit('SET_EMAIL', data.email)
              commit('SET_PHONE', data.phone)
              commit('SET_USERID', data.userId)
              commit('SET_ACCESSTOKEN', data.accessToken)
              commit('SET_CHECKSTATUS', data.checkStatus)
              commit('SET_LEVEL', data.level)
              commit('SET_REGISTERTYPE', data.registerType)
              commit('SET_SOURCE', data.source)
              newFunction(data)
              resolve()
            } else {
              Message({
                message: response.msg,
                type: 'error',
                duration: 5 * 1000
              })
            }
          })
          .catch(error => {
            _this.loginLoad = false
            Message({
              message: '登录失败',
              type: 'error',
              duration: 5 * 1000
            })
            console.log(error)
            reject(error)
          })
      })
    },

    // 获取用户信息
    SetTime({ commit }, time) {
      return new Promise((resolve, reject) => {
        commit('SET_LINKTIME', time)
      })
    },
    // 登出
    LogOut({ commit, state }) {
      // var _this = this
      return new Promise((resolve, reject) => {
        logout(state.enable)
          .then(() => {
            sessionStorage.removeItem('oaxLoginUserId')
            sessionStorage.removeItem('oaxMinioLoginAccessToken')
            sessionStorage.removeItem('oaxLoginName')
            sessionStorage.removeItem('oaxShowName')
            sessionStorage.removeItem('oaxLoginEmail')
            sessionStorage.removeItem('oaxLoginPhone')
            sessionStorage.removeItem('oaxLoginCheckStatus')
            sessionStorage.removeItem('oaxLoginLevel')
            sessionStorage.removeItem('oaxLoginRegisterTypee')
            sessionStorage.removeItem('oaxLoginSource')
            sessionStorage.removeItem('oaxLoginpassword')
            commit('SET_USERID', '')
            commit('SET_SHOWNAME', '')
            commit('SET_EMAIL', '')
            commit('SET_PHONE', '')
            commit('SET_CHECKSTATUS', '')
            commit('SET_LEVEL', '')
            commit('SET_REGISTERTYPE', '')
            commit('SET_SOURCE', '')
            commit('SET_ACCESSTOKEN', '')
            commit('SET_PERMISSIONS', [])
            sessionStorage.removeItem('login')
            commit('SET_MENUS', [])
            resolve()
          })
          .catch(error => {
            reject(error)
          })
      })
    },

    // 前端 登出
    FedLogOut({ commit }) {
      // var _this = this
      return new Promise(resolve => {
        sessionStorage.removeItem('oaxLoginUserId')
        sessionStorage.removeItem('oaxMinioLoginAccessToken')
        sessionStorage.removeItem('oaxLoginName')
        sessionStorage.removeItem('oaxShowName')
        sessionStorage.removeItem('oaxLoginEmail')
        sessionStorage.removeItem('oaxLoginPhone')
        sessionStorage.removeItem('oaxLoginCheckStatus')
        sessionStorage.removeItem('oaxLoginLevel')
        sessionStorage.removeItem('oaxLoginRegisterTypee')
        sessionStorage.removeItem('oaxLoginSource')
        commit('SET_USERID', '')
        commit('SET_SHOWNAME', '')
        commit('SET_EMAIL', '')
        commit('SET_PHONE', '')
        commit('SET_CHECKSTATUS', '')
        commit('SET_LEVEL', '')
        commit('SET_REGISTERTYPE', '')
        commit('SET_SOURCE', '')
        commit('SET_ACCESSTOKEN', '')
        commit('SET_PERMISSIONS', [])
        sessionStorage.removeItem('login')
        commit('SET_MENUS', [])
        resolve()
      })
    }
  }
}

export default user
function newFunction(data) {
  console.log(data.name)
}


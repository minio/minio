// import Cookies from 'js-cookie'

const app = {
  state: {
    language: localStorage.getItem('language') || 'en',
    routerMenu: localStorage.getItem('routerMenu') || 0,
  },
  mutations: {
    SET_LANGUAGE: (state, language) => {
      state.language = language
      localStorage.setItem('language', language)
    },
    SET_ROUTERMENU: (state, routerMenu) => {
      // console.log(state, routerMenu);
      state.routerMenu = routerMenu
      localStorage.setItem('routerMenu', routerMenu)
    }
  },
  actions: {
    setLanguage({ commit }, language) {
      commit('SET_LANGUAGE', language)
    },
    setRouterMenu({ commit }, routerMenu) {
      commit('SET_ROUTERMENU', routerMenu)
    }
  }
}

export default app

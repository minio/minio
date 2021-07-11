import Vue from 'vue'
import VueI18n from 'vue-i18n'
// import Cookies from 'js-cookie'
import elementEnLocale from 'element-ui/lib/locale/lang/en' // 英语
import elementZhCNLocale from 'element-ui/lib/locale/lang/zh-CN' // 简体中文
import elementZhTWLocale from 'element-ui/lib/locale/lang/zh-TW' // 繁体中文
import enLocale from './en'
import zhCNLocale from './cn'
import zhTWLocale from './zh_TW'

Vue.use(VueI18n)

const messages = {
  en: {
    ...enLocale,
    ...elementEnLocale
  },
  cn: {
    ...zhCNLocale,
    ...elementZhCNLocale
  },
  zh_TW: {
    ...zhTWLocale,
    ...elementZhTWLocale
  }
}

const i18n = new VueI18n({
  // locale: localStorage.getItem('language') || 'en', // set locale
  locale: 'en', // set locale
  messages // set locale messages
})

export default i18n

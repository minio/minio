<template>
  <div class="landing" @click="actClient(0)">
      <header class="fe-header">
        <h2>
          <span class="main" v-if="editNameFile" v-for="(item, index) in currentBucketAll" :key="index">
            <a href="javascript:;" @click="buckerAdress(index)">{{item}}</a>
          </span>
          <a href="javascript:;" class="fe-edit" @click="editFun" v-if="editNameFile">
            <el-tooltip class="item" effect="dark" content="Choose or create new path" placement="bottom">
              <i class="iconfont icon-tianjiawenjian"></i>
            </el-tooltip>
          </a>
          <el-input v-model="user.name_file" ref="mark" placeholder="Choose or create new path" v-else @blur="editFileFun"></el-input>
        </h2>
        <div class="feh-used">
          <div class="fehu-chart">
            <div style="width: 0px;"></div>
          </div>
          <ul>
            <li><span>Used: </span>{{userd | formatbytes}}</li>
          </ul>
        </div>
        <ul class="feh-actions">
          <li>
            <div class="dropdown btn-group">
              <button type="button" class="btn btn-default pcIcon" @click.stop="signBtn"><i class="iconfont icon-ziyuan"></i></button>
              <button type="button" class="btn btn-default mobileIcon" @click.stop="signBtn"><i class="el-icon-more"></i></button>
              <ul class="dropdown-menu" v-show="signShow">
                <li @click="handleFullScreen">
                  <a href="javascript:;">
                    Fullscreen <i class="iconfont icon-fangda"></i>
                  </a>
                </li>
                <li @click="openAbout">
                  <a href="javascript:;" id="show-about">About <i class="iconfont icon-i"></i></a>
                </li>
                <li @click="logout">
                  <router-link to="/minio/login" id="logout">Sign Out <i class="iconfont icon-signout"></i></router-link>
                </li>
              </ul>
            </div>
          </li>
        </ul>
      </header>

      <div class="table">
        <el-table
          :data="tableData"
          stripe
          style="width: 100%"
          >
          <el-table-column
            prop="name"
            sortable
            label="Name">
            <template slot-scope="scope">
              <div class="iconBefore" v-if="scope.row.contentType.indexOf('text') >= 0">
                <i class="iconfont el-icon-document" style="background-color: #8a8a8a;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.contentType.indexOf('image') >= 0">
                <i class="iconfont icon-wenjiantupian" style="background-color: #f06292;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.contentType.indexOf('zip') >= 0">
                <i class="iconfont icon-zip" style="background-color: #427089;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.name.indexOf('xlsx') >= 0 || scope.row.contentType.indexOf('excel') >= 0">
                <i class="iconfont icon-exclxlsxlsx" style="background-color: cadetblue;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.name.indexOf('pdf') >= 0">
                <i class="iconfont icon-pdf" style="background-color: #fa7775;font-weight: bold;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.contentType.indexOf('video') >= 0 || scope.row.contentType.indexOf('audio') >= 0">
                <i class="iconfont icon-geshi_tongyongshipin" style="background-color: #f8c363;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.name.indexOf('doc') >= 0">
                <i class="iconfont icon-word1" style="background-color: #2196f5;font-weight: bold;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.name.indexOf('ppt') >= 0">
                <i class="iconfont icon-PPT" style="background-color: #896ea6;font-weight: bold;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.name.indexOf('powerpoint') >= 0 || scope.row.name.indexOf('presentation') >= 0">
                <i class="iconfont icon-ppt" style="background-color: rgb(182, 146, 221);" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
              <div class="iconBefore" v-else-if="scope.row.contentType == ''">
                <i class="el-icon-folder" style="background-color: #a1d6dd;" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                <span style="cursor: pointer;" @click="curListFolder(scope.row.name)">{{scope.row.name | slideName}}</span>
              </div>


              <div class="iconBefore" v-else>
                <i class="iconfont icon-wenjian" @click="drawPlay(scope.$index, true)" v-if="tableData[scope.$index].drawShow"></i>
                <i class="el-icon-check" @click="drawPlay(scope.$index, false)" v-else></i>
                {{scope.row.name | slideName}}
              </div>
            </template>
          </el-table-column>
          <el-table-column
            prop="size"
            label="Size"
            sortable
            width="130">
            <template slot-scope="scope">
              <div class="iconBefore">
                {{scope.row.size | formatbytes}}
              </div>
            </template>

          </el-table-column>
          <el-table-column
            prop="lastModified"
            sortable
            label="Last Modified"
            :formatter="formatter"
            width="180">
          </el-table-column>
          <el-table-column label="" width="80">
            <template slot-scope="scope">
              <span class="point el-icon-more" @click.stop="actClient(scope.$index, 1)"></span>

              <ul class="dropdown-menu" :class="{'dropdown-show': tableData[scope.$index].dropShow}">
                <a href="javascript:;" class="fiad-action" @click="deleteBtn(tableData[scope.$index].name)"><i class="el-icon-delete"></i></a>
                <a href="javascript:;" class="fiad-action" @click="shareBtn(scope.$index)"><i class="el-icon-share"></i></a>
                <a href="javascript:;" class="fiad-action" @click="ShareToFil(tableData[scope.$index])"><img :src="ShareToFilecoin" /></a>
              </ul>
            </template>
          </el-table-column>
        </el-table>
      </div>

      <el-drawer
        :visible.sync="drawer"
        :direction="direction"
        :modal="false"
        :modal-append-to-body="false"
        :before-close="handleClose"
        class="drawStyle01">
        <div class="draw_cont">
          <div class="draw_left"><i class="el-icon-success"></i>  {{drawIndex}} Object selected</div>
          <div class="draw_right">
            <el-button class="btn" @click="deleteDialogVisible = true">Delete selected</el-button>
            <el-button class="btn" @click="downloadFun">{{drawIndex>1?'Download all as zip':'Download object'}}</el-button>
            <el-button type="primary" icon="el-icon-close" class="close" @click="drawPlayClose"></el-button>
          </div>
        </div>
      </el-drawer>

      <div class="model" v-show="openAboutShow">
        <div class="model_bg" @click="openAbout"></div>
        <div class="model_cont">
          <div class="model_close el-icon-close" @click="openAbout"></div>
          <div class="model_left">
            <a href="https://min.io" target="_blank"><img :src="logo" /></a>
          </div>
          <div class="model_right">
            <el-row class="model_ul">
              <el-col :span="24">
                <h2>VERSION</h2>
                <p>{{ aboutServer.MinioVersion }}</p>
              </el-col>
              <el-col :span="24">
                <h2>MEMORY</h2>
                <p>{{ aboutServer.MinioMemory }}</p>
              </el-col>
              <el-col :span="24">
                <h2>PLATFORM</h2>
                <p>{{ aboutServer.MinioPlatform }}</p>
              </el-col>
              <el-col :span="24">
                <h2>RUNTIME</h2>
                <p>{{ aboutServer.MinioRuntime }}</p>
              </el-col>
            </el-row>
          </div>
        </div>
      </div>


      <el-dialog title="" custom-class="customStyle" :before-close="getDialogClose" :visible.sync="dialogFormVisible">
          <el-input v-model="form.name" placeholder="Bucket Name"></el-input>
      </el-dialog>

      <el-dialog
        :visible.sync="deleteDialogVisible"
        custom-class="deleteStyle"
        :show-close="false"
        center>
        <img :src="danger_img" />
        <p>Are you sure you want to delete?</p>
        <h6>This cannot be undone!</h6>
        <div class="btncompose">
          <el-button @click="deleteListFun">Delete</el-button>
          <el-button type="primary" @click="deleteDialogVisible = false">Cancel</el-button>
        </div>
      </el-dialog>

      <share-dialog
        :shareDialog="shareDialog" :shareObjectShow="shareObjectShow"
        :shareFileShow="shareFileShow" :num="num" :share_input="share_input"
        :postAdress="postAdress"
        @getshareDialog="getshareDialog" @getShareGet="getPresignedGet">
      </share-dialog>
  </div>
</template>

<script>
import axios from 'axios'
import Moment from 'moment'
import shareDialog from '@/components/shareDialog.vue';
let that
export default {
  name: 'landing',
  data() {
    return {
      postUrl: this.data_api + `/minio/webrpc`,
      logo: require("@/assets/images/title.svg"),
      ShareToFilecoin: require("@/assets/images/WechatIMG1133.png"),
      danger_img: require("@/assets/images/danger.png"),
      bodyWidth: document.body.clientWidth>600?true:false,
      tableData: [],
      direction: 'ttb',
      drawIndex: 0,
      signShow: false,
      drawer: false,
      fullscreen: false,
      user: {
        name: '',
        userd: 0,
        name_file: ''
      },
      form: {
          name: ''
      },
      editNameFile: true,
      table: true,
      openAboutShow: false,
      aboutPresignedGet: {
        uiVersion: "",
        url: ""
      },
      deleteDialogVisible: false,
      deleteDialogIndex: [],
      currentBucketAll: [],
      presonToken: {
        token: "",
        uiVersion: ""
      },
      prefixName: '',
      browserNameChange: '',
            shareDialog: false,
            shareObjectShow: true,
            shareFileShow: false,
            share_now: null,
            share_input: '',
            num: {
              num_Day: 5,
              num_Hours: 0,
              num_Minutes: 0,
            },
            postAdress: ''
    }
  },
  components: {
      shareDialog
  },
  props: ['aboutServer','aboutListObjects','dialogFormVisible','currentBucket','userd', 'slideListClick'],
  methods: {
    buckerAdress(index) {
      let _this = this
      if(index){
        _this.prefixName = _this.currentBucketAll.slice(1, index+1).join('/')
      }else{
        _this.prefixName = ''
      }
      _this.currentBucketAll = _this.currentBucketAll.slice(0, index+1)
      _this.$emit('getListObjects', _this.currentBucket, _this.prefixName);
    },
    curListFolder(name) {
        let _this = this
        let floder = name.slice(0, name.length-1)
        _this.currentBucketAll.push(floder.replace(_this.prefixName+'/', ""))
        _this.prefixName = _this.currentBucketAll.slice(1).join('/');
        _this.$emit('getListObjects', _this.currentBucket, _this.prefixName);
    },
    ShareToFil (now) {
      this.shareDialog = true
      this.shareObjectShow = false
      this.shareFileShow = true
      this.postAdress = this.currentBucket + '/' + now.name
      //this.$emit('getshareHome', true, false, true);
    },
    getshareDialog(shareDialog) {
      this.shareDialog = shareDialog
    },
    editFun() {
      this.editNameFile = false
      // this.$refs['mark'].focus()
      this.browserNameChange = this.currentBucketAll.join('/')
      this.user.name_file = this.currentBucketAll.join('/') + '/'
      this.$nextTick(() => {
        this.$refs.mark.$el.querySelector('input').focus()
      })
    },
    editFileFun() {
      this.editNameFile = true
    },
    openAbout() {
      this.openAboutShow = !this.openAboutShow
    },
    handleClose(done) {

    },
    formatter(row, column) {
        return row.lastModified;
    },
    actClient(index, now) {
      let _this = this;
      if(_this.tableData && _this.tableData.length > 0) {
        let active = _this.tableData[index].dropShow;
        _this.tableData.map(item => {
          item.dropShow = false;
        })
        if(now){
          _this.tableData[index].dropShow = !active;
        }
      }
      _this.signShow = false;
    },
    deleteBtn(name) {
      this.deleteDialogVisible = true
      this.deleteDialogIndex = []
      this.deleteDialogIndex.push(name)
      // this.tableData = this.tableData.splice(index, 1)
    },
    deleteListFun() {
      let _this = this
      if(_this.deleteDialogIndex) {
        _this.deleteDialogIndex.map(item => {
          _this.getRemoveObject(item)
        })
      }
      _this.drawPlayClose()
    },
    getRemoveObject(obj) {
      let _this = this
      let objArr = []
      objArr.push(obj)
      let dataRemoveObject = {
          id: 1,
          jsonrpc: "2.0",
          method: "web.RemoveObject",
          params:{
              bucketName: _this.currentBucket,
              objects: objArr
          }
      }
      axios.post(_this.postUrl, dataRemoveObject, {headers: {
          'Authorization':"Bearer "+ _this.$store.getters.accessToken
      }}).then((response) => {
          let json = response.data
          let error = json.error
          let result = json.result
          if (error) {
              _this.$message.error(error.message);
              return false
          }

          if(_this.tableData) {
            _this.tableData.map((item, index) => {
              if(obj == item.name){
                _this.tableData.splice(index, 1)
              }
            })
          }
          _this.deleteDialogVisible = false
          _this.$emit('getRemoveObject', _this.tableData);


      }).catch(function (error) {
          console.log(error);
          // console.log(error.message, error.request, error.response.headers);
      });
    },
    downloadFun() {
      let _this = this
      let dataCreateURLToken = {
          id: 1,
          jsonrpc: "2.0",
          method: "web.CreateURLToken",
          params:{}
      }
      axios.post(_this.postUrl, dataCreateURLToken, {headers: {
          'Authorization':"Bearer "+ _this.$store.getters.accessToken
      }}).then((response) => {
          let json = response.data
          let error = json.error
          let result = json.result
          if (error) {
              _this.$message.error(error.message);
              return false
          }
          _this.presonToken = result
          _this.getDownload()
          _this.drawPlayClose()
      }).catch(function (error) {
          console.log(error);
          // console.log(error.message, error.request, error.response.headers);
      });

    },
    getDownload() {
      let _this = this
      let requestUrl = this.data_api + '/minio/download/'+ _this.currentBucketAll[0] +'/'+ _this.deleteDialogIndex[0] +'?token=' + _this.presonToken.token
      let requestZipUrl = this.data_api + `/minio/zip?token=` + _this.presonToken.token
      let objZip = {
        bucketName: _this.currentBucketAll[0],
        objects: _this.deleteDialogIndex,
        prefix: _this.prefixName
      }

      let postUrl = _this.drawIndex>1 ? requestZipUrl : requestUrl

      if(_this.drawIndex>1){
        var anchor = document.createElement("a")
        document.body.appendChild(anchor)

        var xhr = new XMLHttpRequest()
        xhr.open("POST", requestZipUrl, true)
        xhr.responseType = "blob"

        xhr.onload = function (e) {
          if (this.status == 200) {
            var blob = new Blob([this.response], {
              type: "octet/stream",
            })
            var blobUrl = window.URL.createObjectURL(blob)
            var separator = objZip.prefix.length > 1 ? "-" : ""

            anchor.href = blobUrl
            anchor.download = null ||
              objZip.bucketName + separator + objZip.prefix.slice(0, -1) + ".zip"

            anchor.click()
            window.URL.revokeObjectURL(blobUrl)
            anchor.remove()
          }
        }
        xhr.send(JSON.stringify(objZip))
      }else{
        var a = document.createElement("a");
        a.download = _this.currentBucketAll[0] + ".csv";
        a.href = requestUrl;
        $("body").append(a); // 修复firefox中无法触发click
        a.click();
        $(a).remove();
      }
    },
    shareBtn(index) {
      let _this = this
      _this.share_now = index

      _this.getPresignedGet()

      _this.shareDialog = true
      _this.shareObjectShow = true
      _this.shareFileShow = false
    },
    getPresignedGet() {
      let _this = this
      _this.$message({
          message: 'Object shared. Expires in '+_this.num.num_Day+' days '+_this.num.num_Hours+' hours '+_this.num.num_Minutes+' minutes',
          type: 'success',
          showClose: true,
          customClass: 'messageTip'
      });
      let expiry = _this.num.num_Day * 24 * 60 * 60 + _this.num.num_Hours * 60 * 60 + _this.num.num_Minutes * 60
      let dataPresignedGet = {
          id: 1,
          jsonrpc: "2.0",
          method: "web.PresignedGet",
          params:{
              host: location.host,
              bucket: _this.currentBucket,
              object: _this.tableData[_this.share_now].name,
              expiry: expiry
          }
      }
      axios.post(_this.postUrl, dataPresignedGet, {headers: {
          'Authorization':"Bearer "+ _this.$store.getters.accessToken
      }}).then((response) => {
          let json = response.data
          let error = json.error
          let result = json.result
          if (error) {
              _this.$message.error(error.message);
              return false
          }
          _this.aboutPresignedGet = result
          _this.share_input = `${window.location.protocol}//` + _this.aboutPresignedGet.url
          _this.shareDialog = true

      }).catch(function (error) {
          console.log(error);
          // console.log(error.message, error.request, error.response.headers);
      });
    },
    drawPlay(index, now) {
      let _this = this;
      _this.tableData[index].drawShow = !now;
      if(!now){
        let i = 0;
        if(_this.tableData) {
          _this.tableData.map(item => {
            if(!item.drawShow){
              i += 1;
              _this.drawIndex -= 1;
            }
          })
        }
        if(i<1){
          _this.drawer = false;
          return false
        }
      }else{
        _this.drawIndex = 0;
        _this.deleteDialogIndex = [];
        if(_this.tableData) {
          _this.tableData.map(item => {
            if(!item.drawShow){
              _this.drawIndex += 1;
              _this.deleteDialogIndex.push(item.name)
            }
          })
        }
        _this.drawer = true;
      }
    },
    drawPlayClose() {
      let _this = this;
      if(_this.tableData) {
        _this.tableData.map(item => {
          item.drawShow = true;
        })
      }
      _this.drawIndex = 0;
      _this.deleteDialogIndex = [];
      _this.drawer = false
    },
    signBtn() {
      this.signShow = !this.signShow
    },
    handleFullScreen(){
      let element = document.documentElement;
      if (this.fullscreen) {
        if (document.exitFullscreen) {
          document.exitFullscreen();
        } else if (document.webkitCancelFullScreen) {
          document.webkitCancelFullScreen();
        } else if (document.mozCancelFullScreen) {
          document.mozCancelFullScreen();
        } else if (document.msExitFullscreen) {
          document.msExitFullscreen();
        }
      } else {
        if (element.requestFullscreen) {
          element.requestFullscreen();
        } else if (element.webkitRequestFullScreen) {
          element.webkitRequestFullScreen();
        } else if (element.mozRequestFullScreen) {
          element.mozRequestFullScreen();
        } else if (element.msRequestFullscreen) {
          // IE11
          element.msRequestFullscreen();
        }
      }
      this.fullscreen = !this.fullscreen;
    },
    // 退出登录
    logout() {
      var _this = this;
      _this.$store.dispatch("FedLogOut").then(() => {
        _this.$router.replace({ name: 'login' })
      });
    },
    getDialogClose() {
        let _this = this;
        _this.$emit('getDialogClose', false);
        _this.form.name = ''
    },
    getServer() {
        let _this = this;
        _this.$emit('getaboutServer', _this.form.name, false);
        _this.form.name = ''
    },
    aboutListData(){
      let _this = this
      if(_this.aboutListObjects && _this.aboutListObjects.objects){
          _this.aboutListObjects.objects.map(item => {
            item.drawShow = true
            item.dropShow = false
            item.lastModified = Moment(item.lastModified).format('YYYY-MM-DD HH:mm:ss')
          })
          _this.tableData = JSON.parse(JSON.stringify(_this.aboutListObjects.objects))
          //console.log('tableData', _this.tableData)
      }else{
        _this.tableData = JSON.parse(JSON.stringify(_this.aboutListObjects.objects))
      }
    }
  },
  watch: {
    aboutListObjects: function(){
      let _this = this
      _this.aboutListData()
    },
    'currentBucket': function(){
      let _this = this
      _this.currentBucketAll = _this.currentBucket.split('/')
    },
    'slideListClick': function(){
      let _this = this
      _this.currentBucketAll = _this.currentBucket.split('/')
      _this.prefixName = ''
    }
  },
  filters: {
    formatbytes: function (bytes) {
      if (bytes === 0) return '0 B';
      var k = 1000, // or 1024
          sizes = ['bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'],
          i = Math.floor(Math.log(bytes) / Math.log(k));

      return (bytes / Math.pow(k, i)).toPrecision(3) + ' ' + sizes[i];
    },
    slideName: function (name) {
       if (!name) return '-';
       let retName = that.prefixName ? name.replace(that.prefixName+'/', "") : name
       return retName;
     }
  },
  mounted() {
    let _this = this
    that = _this
    _this.aboutListData()
    _this.currentBucketAll = _this.currentBucket.split('/')
    document.onkeydown = function(e) {
      if (e.keyCode === 13) {
        if(!_this.editNameFile){
          _this.$nextTick(() => {
            _this.editNameFile = true
            _this.$refs.mark.$el.querySelector('input').blur()
          })
        }else if(_this.form.name){
            console.log('create bucket')
            _this.getServer();
        }
        if(_this.user.name_file && !_this.editNameFile){
          if(_this.user.name_file){
              console.log('bucket prefix:', _this.user.name_file.split('/'))
              let fileAll = _this.user.name_file.split('/')
              _this.currentBucketAll = []
              if(fileAll) {
                fileAll.map(item => {
                  if(item){
                    _this.currentBucketAll.push(item)
                  }
                })
              }
              _this.prefixName = _this.currentBucketAll.slice(1).join('/');
              console.log('prefix:', _this.prefixName, 'currentBucketAll:', _this.currentBucketAll[0]);
              if(_this.currentBucketAll[0] == _this.currentBucket){
                _this.$emit('getListObjects', _this.currentBucket, _this.prefixName);
              }else{
                _this.$emit('getaboutServer', _this.currentBucketAll[0], false, _this.prefixName, _this.browserNameChange);
              }
          }else{
            _this.prefixName = ''
          }
        }
      }
    }
  }
}
</script>

<style lang="scss" scoped>
.landing{
  height: 100%;
  .fe-header {
    position: relative;
    padding: 0.4rem 0.4rem 0.4rem 0.45rem;
    h2 {
      width: calc(100% - 60px);
      font-size: 0.16rem;
      font-weight: 400;
      margin: 0;
      display: flex;
      align-items: center;
      span {
          display: inline-block;
          a{
            color: #46a5e0;
            font-size: 0.15rem;
          }
          &:last-of-type:after, &:not(:first-child):before{
            content: "/";
            margin: 0 4px;
            color: #8e8e8e;
          }
      }
      .fe-edit {
          font-size: 0.2rem;
          color: #46a5e0;
          margin-left: 4px;
          i{
            font-size: 0.2rem;
          }
      }
      .el-input /deep/{
        .el-input__inner{
          padding: 0;
          color: #46a5e0;
          font-size: 0.15rem;
          border: 0;
          border-bottom: 1px solid #DCDFE6;
          border-radius: 0;
        }
      }
    }
    .feh-used {
        margin-top: 12px;
        max-width: 285px;
        .fehu-chart {
            height: 5px;
            background: #eee;
            position: relative;
            border-radius: 2px;
            overflow: hidden;
        }
        ul {
            margin-top: 7px;
            list-style: none;
            padding: 0;
            font-size: 0.15rem;
            line-height: 1.42857143;
            color: #8e8e8e;
            li{
              float: left;
              padding-right: 0;
              display: inline-block;
              font-size: 0.145rem;
            }
        }
    }
    .feh-actions {
        list-style: none;
        padding: 0;
        margin: 0;
        position: absolute;
        right: 35px;
        top: 30px;
        z-index: 22;
        .btn-group>button, >a {
            display: block;
            height: 45px;
            min-width: 45px;
            text-align: center;
            border-radius: 50%;
            padding: 0;
            border: 0;
            background: none;
            color: #7b7b7b;
            font-size: 21px;
            line-height: 45px;
            transition: all;
            transition-duration: .3s;
            cursor: pointer;
            &:hover{
              background-color: rgba(0,0,0,0.1);
            }
            i{
              font-size: 0.23rem;
              font-weight: 900;
            }
        }
        .mobileIcon{
          display: none !important;
        }
        li {
            width: 100%;
            display: inline-block;
            text-align: right;
            vertical-align: top;
            line-height: 100%;
            .dropdown, .dropup {
                position: relative;
            }
            .dropdown-menu {
                position: absolute;
                top: 100%;
                left: auto;
                right: 0;
                z-index: 1000;
                float: left;
                min-width: 160px;
                padding: 5px 0;
                margin: 2px 0 0;
                list-style: none;
                font-size: 15px;
                text-align: left;
                background-color: #fff;
                border: 1px solid transparent;
                border-radius: 4px;
                box-shadow: 0 6px 12px rgba(0,0,0,0.18);
                background-clip: padding-box;

                padding: 15px 0;
                top: 0;
                margin-top: -1px;
                display: flex;
                flex-wrap: wrap;
                li>a {
                    display: block;
                    padding: 0.07rem 0.2rem;
                    clear: both;
                    font-weight: 400;
                    line-height: 1.42857143;
                    color: #8e8e8e;
                    white-space: nowrap;
                    i {
                        width: 20px;
                        position: relative;
                        top: 1px;
                        padding: 0 0 0 0.1rem;
                        color: #8e8e8e;
                        font-weight: 600;
                    }
                    &:hover{
                      text-decoration: none;
                      color: #333;
                      background-color: rgba(0,0,0,.05);
                    }
                }
            }
        }
    }
  }
  .table{
    width: 100%;
    // overflow-x: scroll;
    .el-table /deep/{
      // min-width: 500px;
      .el-table__header-wrapper{
        margin-bottom: 0.2rem;
      }
      th{
        >.cell{
          font-weight: 500;
          color: #818181;
          font-size: 0.15rem;
        }
      }
      th,td{
        &:nth-child(1){
          padding-left: 0.3rem;
        }
      }
      .descending {
        .sort-caret.descending{

        }
      }
      .cell{
        cursor: default;
        line-height: 0.35rem;
        .point{
          display: block;
          margin: auto;
          text-align: center;
          font-size: 0.18rem;
          color: #818181;
          cursor: pointer;
          &:hover{
            color: #333;
          }
        }
        .dropdown-menu {
          position: absolute;
          top: 100%;
          left: 0;
          z-index: 1000;
          display: none;
          float: left;
          min-width: 160px;
          padding: 5px 0;
          margin: 2px 0 0;
          list-style: none;
          font-size: 15px;
          text-align: left;
          background-color: #fff;
          border: 1px solid transparent;
          border-radius: 4px;
          box-shadow: 0 6px 12px rgba(0,0,0,0.18);
          background-clip: padding-box;

          .fiad-action {
              height: 0.35rem;
              width: 0.35rem;
              background: #ffc107;
              display: inline-block;
              border-radius: 50%;
              text-align: center;
              line-height: 0.35rem;
              font-weight: 400;
              position: relative;
              top: 12px;
              margin-left: 5px;
              animation-name: fiad-action-anim;
              transform-origin: center center;
              -webkit-backface-visibility: none;
              backface-visibility: none;
              box-shadow: 0 2px 4px rgba(0,0,0,0.1);
              display: flex;
              float: right;
              align-items: center;
              justify-content: center;
              i {
                  font-size: 0.18rem;
                  font-weight: bold;
                  color: #fff;
              }
              img {
                  display: block;
                  width: 100%;
                  height: 100%;
              }
          }
        }
        .dropdown-show{
          display: block;
          background-color: transparent;
          box-shadow: none;
          padding: 0;
          right: 0.6rem;
          top: 0;
          left: auto;
          margin: 0;
          height: 100%;
          text-align: right;
        }
        .iconBefore{
          line-height: 0.35rem;
          i{
            float: left;
            display: flex;
            justify-content: center;
            align-items: center;
            width: 0.35rem;
            height: 0.35rem;
            margin: 0 0.1rem 0 0;
            background-color: #32393f;
            font-size: 0.18rem;
            border-radius: 50%;
            color: #fff;
            cursor: pointer;
            font-weight: bold;
          }
          .iconfont{
            background-color: #afafaf;
            font-weight: normal;
            &:hover::before{
              content: "\e600";
              font-size: 0.16rem;
            }
          }
        }
      }
    }
    // &::-webkit-scrollbar{
    //     width: 7px;
    //     height: 7px;
    //     background-color: #F5F5F5;
    // }

    // /*定义滚动条轨道 内阴影+圆角*/
    // &::-webkit-scrollbar-track {
    //     box-shadow: inset 0 0 6px rgba(0, 0, 0, 0.3);
    //     -webkit-box-shadow: inset 0 0 6px rgba(0, 0, 0, 0.3);
    //     border-radius: 10px;
    //     background-color: #F5F5F5;
    // }

    // /*定义滑块 内阴影+圆角*/
    // &::-webkit-scrollbar-thumb{
    //     border-radius: 10px;
    //     box-shadow: inset 0 0 6px rgba(0, 0, 0, .1);
    //     -webkit-box-shadow: inset 0 0 6px rgba(0, 0, 0, .1);
    //     background-color: #c8c8c8;
    // }
  }
  .el-drawer__wrapper.drawStyle01 /deep/{
    bottom: auto;
    height: auto;
    padding: 20px 20px 20px 25px;
    background-color: #2298F6;
    z-index: 20;
    box-shadow: 0 0 10px rgba(0,0,0,0.3);
    text-align: center;
    .el-drawer.ltr, .el-drawer.rtl, .el-drawer__container{
      height: 0.35rem;
      line-height: 0.35rem;
    }
    .el-drawer{
        position: relative;
        height: 100% !important;
        background: transparent;
        box-shadow: none;
    }
    .el-drawer__header{
      display: none;
    }
    .el-drawer__body{
      font-size: 0.16rem;
      .draw_cont{
        display: flex;
        justify-content: space-between;
        align-items: center;
        color: #fff;
        line-height: 0.35rem;
        .draw_left{
          display: flex;
          justify-content: space-between;
          align-items: center;
          i{
            margin-right: 10px;
            font-size: 0.23rem;
          }
        }
        .draw_right{
          .el-button{
            font-size: 0.16rem;
            i{
              font-weight: bold;
            }
          }
          .btn{
            padding: 0.08rem;
            background-color: transparent;
            border: 2px solid hsla(0,0%,100%,.9);
            color: #fff;
            border-radius: 2px;
            padding: 5px 10px;
            font-size: 0.13rem;
            transition: all;
            transition-duration: .3s;
            margin-left: 10px;
            &:hover{
              color: #2298F6;
              background-color: #fff;
            }
          }
          .close{
            padding: 0.06rem;
            font-weight: bold;
            border-radius: 50%;
          }
        }
      }
    }
  }
  .model{
    display: flex;
    justify-content: center;
    align-items: center;
    position: fixed;
    top: 0;
    right: 0;
    left: 0;
    bottom: 0;
    z-index: 9;
    .model_bg{
      position: absolute;
      left: 0;
      top: 0;
      width: 100%;
      height: 100%;
      background-color: rgba(0,0,0,0.1);
      z-index: 10;
    }
    .model_cont{
      display: flex;
      position: relative;
      width: 600px;
      min-height: 300px;
      background-color: #00303f;
      z-index: 11;
      .model_close{
        right: 0.15rem;
        font-weight: 400;
        opacity: 1;
        font-size: 0.17rem;
        position: absolute;
        text-align: center;
        top: 0.15rem;
        z-index: 1;
        padding: 0;
        border: 0;
        background-color: hsla(0,0%,100%,.1);
        color: hsla(0,0%,100%,.8);
        width: 0.25rem;
        height: 0.25rem;
        display: block;
        border-radius: 50%;
        line-height: 0.25rem;
        text-shadow: none;
        cursor: pointer;
        &:hover{
          background-color: hsla(0,0%,100%,.2);
        }
      }
      .model_left{
        display: flex;
        justify-content: center;
        align-items: center;
        background-color: #022631;
        width: 150px;
        img{
          width: 70px;
        }
      }
      .model_right{
        display: flex;
        justify-content: center;
        align-items: center;
        width: calc(100% - 150px - 0.6rem);
        padding: 0.3rem;
        .el-row /deep/{
          width: 100%;
          .el-col{
            width: 100%;
            margin-bottom: 0.15rem;
            line-height: 1.42857143;
            h2{
              color: hsla(0,0%,100%,.8);
              text-transform: uppercase;
              font-size: 0.14rem;
              font-weight: normal;
              line-height: 2;
            }
            p{
              font-size: 0.13rem;
              color: hsla(0,0%,100%,.4);
            }
          }
        }
      }
    }
  }
  .el-dialog__wrapper /deep/{
    justify-content: center;
    display: flex;
    align-items: center;

    .el-dialog.customStyle{
        width: 400px;
        margin: 0 !important;
        position: absolute;
        bottom: 0.9rem;
        right: 0.2rem;
        .el-dialog__body{
            padding: 0.2rem 0.3rem 0.3rem;
            .el-input{
                .el-input__inner{
                    border: 0;
                    border-bottom: 1px solid #DCDFE6;
                    border-radius: 0;
                    text-align: center;
                    font-size: 0.13rem;
                    color: #32393f;
                }
            }
        }
    }
    .deleteStyle{
        width: 90%;
        max-width: 400px;
      .el-dialog__header{
          display: flex;
          .el-dialog__title{
              font-size: 0.15rem;
              color: #333;
          }
      }
      .el-dialog__body{
        padding: 0 0.2rem 0.2rem;
        img{
          display: block;
          width: 0.7rem;
          margin: 0 auto 0.05rem;
        }
        p{
          text-align: center;
          font-size: 0.15rem;
          line-height: 1.5;
          color: #333;
        }
        h6{
          font-size: 0.13rem;
          font-weight: normal;
          color: #bdbdbd;
          margin-top: 5px;
          text-align: center;
        }
        .btncompose{
              display: flex;
              align-items: center;
              justify-content: center;
              margin: 0.25rem auto 0.2rem;
              .el-button{
                padding: 0.05rem 0.1rem;
                margin: 0 0.03rem;
                font-size: 12px;
                color: #fff;
                border: 0;
                background-color: #ff726f;
                line-height: 1.5;
                border-radius: 0.02rem;
                text-align: center;
                transition: all;
                transition-duration: .3s;
                &:last-child{
                  color: #545454;
                  background-color: #eee;
                }
              }
        }
      }
    }
  }
}
@media screen and (max-width:999px){
.landing{
  .fe-header{
    .feh-actions{
        top: 0.1rem;
        right: 0;
        position: fixed;
        .btn-group>button, >a{
          color: #fff;
        }
        .pcIcon{
          display: none !important;
        }
        .mobileIcon{
          display: block !important;
          i{
            font-size: 0.16rem !important;
          }
        }
    }
  }
}
}

@media screen and (max-width:600px){
.landing{
  .model{
    .model_cont{
      width: 90%;
      .model_left{
        display: none;
      }
      .model_right{
        width: 92%;
        padding: 4%;
      }
    }
  }
  .el-dialog__wrapper /deep/{
    .ShareObject{
      width: 90%;
      .el-dialog__body{
        padding: 0;
        .shareContent{
          flex-wrap: wrap;
          .el-row{
            width: 100%;
          }
        }
      }
    }
  }
  .table{
    overflow-x: auto;
    .el-table /deep/{
      min-width: 800px !important;
    }
  }
}
}
</style>


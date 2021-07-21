<template>
    <div class="wrapper" @click="wrapperClick">
        <v-slide :class="{'sliMobile': slideShow}"
            :minioListBuckets="minioListBuckets" :currentBucket="currentBucket"
            :homeClick="homeClick" @homeClickFun="homeClickFun" @getshareHome="getshareHome"
            @getminioListBucket="getminioListBucket" @getListBuckets="getListBuckets"></v-slide>
        <div class="content">
            <el-row class="headStyle">
                <el-col :span="6">
                    <el-button class="iconfont icon-ziyuan" @click.stop="slideBtn" v-if="!slideShow"></el-button>
                    <el-button class="el-icon-back" style="background-color: #484b4e;" @click.stop="slideBtn" v-else></el-button>
                </el-col>
                <el-col :span="12">
                    <img :src="logo" />
                </el-col>
                <el-col :span="6"></el-col>
            </el-row>
            <transition name="move" mode="out-in">
                <router-view
                :aboutServer="aboutServer" :aboutListObjects="aboutListObjects"
                :slideListClick="slideListClick"
                :dialogFormVisible="dialogFormVisible" :currentBucket="currentBucket" :userd="userd"
                @getDialogClose="getDialogClose"
                @getaboutServer="getMakeBucket"
                @getRemoveObject="getRemoveObject"
                @getListObjects="getListObjects"></router-view>
            </transition>
            <div class="addFile">
                <el-row v-if="addFileShow">
                    <el-col :span="24">
                        <!-- <el-upload
                            class="upload-demo"
                            ref="uploadFileRef"
                            :action="actionUrl"

                            multiple
                            :http-request="uploadFile"
                            :file-list="fileList"
                            :on-change="handleChange">
                            <el-tooltip class="item" effect="dark" slot="trigger" content="Upload file" placement="left">
                                <i class="iconfont icon-shangchuan"></i>
                            </el-tooltip>
                        </el-upload> -->

                        <el-upload
                            class="upload-demo"
                            action="customize"
                            ref="uploadFile"
                            :http-request="httpRequest"
                            :on-change="onChange"
                            multiple
                            :auto-upload="false"
                            >
                            <el-tooltip class="item" effect="dark" content="Upload file" placement="left">
                                <i class="iconfont icon-shangchuan"></i>
                            </el-tooltip>
                        </el-upload>
                    </el-col>
                    <el-col :span="24">
                        <el-tooltip class="item" effect="dark" content="Create bucket" placement="left" @click.native="dialogFormVisible = true">
                            <i class="iconfont icon-harddriveyingpan"></i>
                        </el-tooltip>
                    </el-col>
                </el-row>
                <i class="el-icon-plus" :class="{'el-icon-plus-new': addFileShow}" @click.stop="addToggle"></i>
            </div>

            <div class="progressStyle" v-show="drawer">
                <!--:color="customColor"-->
                <!--el-progress :percentage="percentage_new" style="width: 100%;"></el-progress-->
                <progress id="progressBar01" value="0" max="100" style="width: 100%;"></progress>
                <div class="speed">
                  <span id="time"></span><span id="percentage"></span>
                </div>
            </div>
            <el-backtop target=".wrapper"></el-backtop>
        </div>

        <share-dialog
          :shareDialog="shareDialog" :shareObjectShow="shareObjectShow"
          :shareFileShow="shareFileShow" :postAdress="currentBucket"
          @getshareDialog="getshareDialog">
        </share-dialog>
    </div>
</template>

<script>
import axios from 'axios'
import vSlide from './Slide.vue';
import Moment from "moment"
import shareDialog from '@/components/shareDialog.vue';
export default {
    data() {
        return {
            postUrl: this.data_api + `/minio/webrpc`,
            logo: require("@/assets/images/title.svg"),
            bodyWidth: document.body.clientWidth<=1024?true:false,
            addFileShow: false,
            dialogFormVisible: false,
            form: {
                name: ''
            },
            slideShow: false,
            minioListBuckets: {
                buckets: [],
                uiVersion: ""
            },
            currentBucket: 'nbai',
            minioStorageInfo: {
                storageInfo: {},
                uiVersion: ""
            },
            userd: 0,
            aboutServer: {
                MinioVersion:"",
                MinioMemory:"",
                MinioPlatform:"",
                MinioRuntime:"",
                MinioGlobalInfo:{}
            },
            aboutListObjects: {
                objects: [],
                uiVersion: "",
                writable: true
            },
            fileList: [],
            actionUrl: '',
            prefixData: '',
            homeClick: false,
            addArr: [],
            progressArr: {
              ot: 0,
              oloaded: 0
            },
            percentage_new: 0,
            drawer: false,
            customColor: '#5cb87a',
            shareDialog: false,
            shareObjectShow: true,
            shareFileShow: false,
            slideListClick: 0
        }
    },
    components: {
        vSlide,
        shareDialog
    },
    computed: {
        headertitle() {
            return this.$store.getters.headertitle
        },
        routerMenu() {
            return this.$store.getters.routerMenu
        },
    },
    methods: {
        getshareDialog(shareDialog) {
          this.shareDialog = shareDialog
        },
        getshareHome(shareDialog, shareObjectShow, shareFileShow){
          this.shareDialog = shareDialog
          this.shareObjectShow = shareObjectShow
          this.shareFileShow = shareFileShow
        },
        getData() {
            this.getListBuckets()
            this.getStorageInfo()
            this.getServerInfo()
            // let postUrl = `${window.location.protocol}//${window.location.host}/minio/webrpc`
        },
        getListBuckets(name) {
            let _this = this
            // let postUrl = _this.data_api + `/minio/webrpc`
            let dataListBuckets = {
                id: 1,
                jsonrpc: "2.0",
                method: "web.ListBuckets",
                params:{}
            }

            axios.post(_this.postUrl, dataListBuckets, {headers: {
                'Authorization': "Bearer "+ _this.$store.getters.accessToken
            }}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    return false
                }
                _this.minioListBuckets = result
                _this.currentBucket = _this.minioListBuckets && _this.minioListBuckets.buckets?_this.minioListBuckets.buckets[0].name:''

                if(name) {
                  _this.getListObjects(name)
                  return false
                }
                if(_this.minioListBuckets.buckets){
                  _this.getListObjects()
                }
                //console.log('minioListBuckets home', _this.minioListBuckets)

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
            });
        },
        getStorageInfo() {
            let _this = this;
            let dataStorageInfo = {
                id: 1,
                jsonrpc: "2.0",
                method: "web.StorageInfo",
                params:{}
            }
            axios.post(_this.postUrl, dataStorageInfo, {headers: {
                'Authorization': "Bearer "+ _this.$store.getters.accessToken
            }}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    return false
                }
                _this.minioStorageInfo = result
                _this.userd = result.used
                //console.log(json, 'userd:', _this.userd)

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
            });
        },
        getServerInfo() {
            let _this = this
            let dataServerInfo = {
                id: 1,
                jsonrpc: "2.0",
                method: "web.ServerInfo",
                params:{}
            }
            axios.post(_this.postUrl, dataServerInfo, {headers: {
                'Authorization': "Bearer "+ _this.$store.getters.accessToken
            }}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    return false
                }
                _this.aboutServer = result
                //console.log(json)

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
            });
        },
        getListObjects(listName, prefixData) {
            let _this = this
            _this.prefixData = prefixData
            let dataListObjects = {
                id: 1,
                jsonrpc: "2.0",
                method: "web.ListObjects",
                params:{
                    bucketName: listName?listName:_this.minioListBuckets.buckets?_this.minioListBuckets.buckets[0].name:'',
                    prefix: _this.prefixData?_this.prefixData + '/':""
                }
            }
            _this.currentBucket = listName?listName:_this.minioListBuckets.buckets?_this.minioListBuckets.buckets[0].name:''
            axios.post(_this.postUrl, dataListObjects, {headers: {
                'Authorization':"Bearer "+ _this.$store.getters.accessToken
            }}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    return false
                }
                _this.aboutListObjects = result
                //console.log(json)

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
            });
        },
        getDialogClose(dialogFormVisible) {
            this.dialogFormVisible = dialogFormVisible
        },
        getMakeBucket(name, dialogFormVisible, prefix, oldName) {
            let _this = this
            let dataMakeBucket = {
                id: 1,
                jsonrpc: "2.0",
                method: "web.MakeBucket",
                params:{
                    bucketName: name
                }
            }
            _this.dialogFormVisible = dialogFormVisible
            _this.currentBucket = name
            axios.post(_this.postUrl, dataMakeBucket, {headers: {
                'Authorization':"Bearer "+ _this.$store.getters.accessToken
            }}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    if(oldName) {
                      _this.currentBucket = oldName
                      //console.log('error', oldName)
                    }
                    return false
                }
                if(_this.minioListBuckets && _this.minioListBuckets.buckets) {
                  _this.minioListBuckets.buckets.map(item => {
                    if(item.name.indexOf(name) >= 0){
                      _this.getListBuckets()
                      return false
                    }
                  })
                }
                _this.getListBuckets(name)
                _this.getListObjects(name)

                if(prefix){
                    _this.getListObjects(name, false, prefix)
                }

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
            });

        },
        getRemoveObject(data){
            let _this = this
            _this.aboutListObjects.objects = JSON.parse(JSON.stringify(data))
            // console.log(_this.aboutListObjects.objects);
        },
        getminioListBucket(listName) {
            this.getListObjects(listName)
            this.slideListClick += 1
        },
        addToggle() {
           this.addFileShow = !this.addFileShow
        },
        slideBtn() {
            this.slideShow = !this.slideShow
        },
        wrapperClick() {
            this.addFileShow = false
            this.homeClick = false
            this.slideShow = false
        },
        homeClickFun(now) {
            this.homeClick = now
        },


    //文件上传
    httpRequest(file) {
      console.log('httpRequest', file);
    },
    onChange(file, fileList) {
      console.log('onChange', file, fileList);
        let _this = this
        let $hgh
        if(!_this.minioListBuckets.buckets || _this.minioListBuckets.buckets.length < 1){
            _this.$message({
                message: 'Please choose a bucket before trying to upload files.',
                type: 'error',
                showClose: true,
                duration: 0
            });
            $hgh = true
            return false
        }

        if(!$hgh) {
          let prefix = _this.prefixData ? _this.prefixData + '/': ''
          let postUrl = _this.data_api + '/minio/upload/' + _this.currentBucket + '/' + prefix + file.name
          let formData = new FormData();  //创建空对象
          /*for (let i = 0; i < fileList.length; i++) {
            formData.append("Content-Type", fileList[i].raw.type);
            formData.append("Authorization", "Bearer "+ _this.$store.getters.accessToken);
          }*/

              document.getElementById("progressBar01").value = 0
              let xhr
              xhr = new XMLHttpRequest()
              xhr.open("PUT", postUrl, true)
              xhr.withCredentials = false
              const token = _this.$store.getters.accessToken
              if (token) {
                xhr.setRequestHeader(
                  "Authorization",
                  "Bearer " + _this.$store.getters.accessToken
                )
              }
              xhr.setRequestHeader(
                "x-amz-date",
                Moment()
                  .utc()
                  .format("YYYYMMDDTHHmmss") + "Z"
              )

              //console.log('dispatch', xhr, file.size, file.name);

              xhr.onload = function(event) {
                //console.log('jinru1', xhr.status);
                if (xhr.status == 401 || xhr.status == 403) {
                  _this.$message({
                      message: "Unauthorized request.",
                      type: 'danger'
                  });
                }
                if (xhr.status == 500) {
                  _this.$message({
                      message: xhr.responseText,
                      type: 'danger'
                  });
                }
                if (xhr.status == 200) {
                    _this.$message({
                        message: "File '" + file.name + "' uploaded successfully.",
                        type: 'success'
                    });

                    _this.getListObjects(_this.currentBucket, _this.prefixData)
                }

                xhr.upload.addEventListener("error", event => {
                    _this.$message({
                        message: "Error occurred uploading '" + file.name + "'.",
                        type: 'danger'
                    });
                })

                xhr.upload.addEventListener("progress", event => {
                  if (event.lengthComputable) {
                    let loaded = event.loaded
                    let total = event.total
                    // Update the counter
                    //dispatch(updateProgress(slug, loaded))
                  }
                })

                //xhr.send(file.raw)
             }

             xhr.upload.onprogress = _this.progressFunction;//【上传进度调用方法实现】
             xhr.upload.onloadstart = function(){//上传开始执行方法
                 //$(".progressStyle").html("")
                 _this.progressArr.ot = new Date().getTime();   //设置上传开始时间
                 _this.progressArr.oloaded = 0;//设置上传开始时，以上传的文件大小为0
                 _this.percentage_new = 0
                 _this.drawer = true
                  //$(".progressStyle").append('<el-progress :percentage="percentage_new" id="progressBar" value="0" max="100" style="width: 100%;"></el-progress><progress id="progressBar01" value="0" max="100" style="width: 100%;"></progress><div class="speed"><span id="time"></span>(<span id="percentage"></span>)</div>')
             };
             xhr.send(file.raw)

          /*
          axios.put(postUrl, formData, {headers: {
              'Authorization':"Bearer "+ _this.$store.getters.accessToken,
              'Content-Type': file.raw.type
          }}).then((response) => {
              let json = response.data
              //console.log('upload res', json)

          }).catch(function (error) {
              console.log(error);
              // console.log(error.message, error.request, error.response.headers);
          });*/




          /*if(fileList){
            fileList.map(item => {
              $("#progressD").append('<div class="progressStyle"><el-progress :percentage="percentage_new" id="progressBar" value="0" max="100" style="width: 100%;"></el-progress><progress id="progressBar01" value="0" max="100" style="width: 100%;"></progress><div class="speed"><span id="time"></span>(<span id="percentage"></span>)</div></div>')
            })
          }
          fileList.map(item => {
             _this.$notify({
               title: '',
               duration: 0,
               dangerouslyUseHTMLString: true,
               position: 'bottom-right',
               message: '<div class="progressStyle"><el-progress :percentage="percentage_new" id="progressBar" value="0" max="100" style="width: 100%;"></el-progress><progress id="progressBar01" value="0" max="100" style="width: 100%;"></progress><div class="speed"><span id="time"></span>(<span id="percentage"></span>)</div></div>'
             });
          })*/
        }
      },
      //上传进度实现方法，上传过程中会频繁调用该方法
      progressFunction(evt) {
           let _this = this
           let progressBar = document.getElementById("progressBar01");
           let percentageDiv = document.getElementById("percentage");
           if (evt.lengthComputable) {//
               progressBar.max = evt.total;
               progressBar.value = evt.loaded;
               _this.percentage_new = Math.round(evt.loaded / evt.total * 100);
               percentageDiv.innerHTML = "(" + Math.round(evt.loaded / evt.total * 100) + "%)";
           }

          let time = document.getElementById("time");
          let nt = new Date().getTime();//获取当前时间
          var pertime = (nt - _this.progressArr.ot)/1000; //计算出上次调用该方法时到现在的时间差，单位为s
          _this.progressArr.ot = new Date().getTime(); //重新赋值时间，用于下次计算

          var perload = evt.loaded - _this.progressArr.oloaded; //计算该分段上传的文件大小，单位b
          _this.progressArr.oloaded = evt.loaded;//重新赋值已上传文件大小，用以下次计算

          //上传速度计算
          var speed = perload/pertime;//单位b/s
          var bspeed = speed;
          var units = 'b/s';//单位名称
          if(speed/1024>1){
              speed = speed/1024;
              units = 'k/s';
          }
          if(speed/1024>1){
              speed = speed/1024;
              units = 'M/s';
          }
          speed = speed.toFixed(1);
          //剩余时间
          var resttime = ((evt.total-evt.loaded)/bspeed).toFixed(1);
          time.innerHTML = speed+units;  //+'，剩余时间：'+resttime+'s'
          if(bspeed==0)
              time.innerHTML = '上传已取消';
          if(!resttime || resttime <= 0){
            //Notification.closeAll()
            _this.drawer = false
          }
      }


    },
    mounted() {
        let _this = this
        _this.getData()
    },
};
</script>

<style lang="scss" scoped>
.wrapper{
    display: flex;
    flex-wrap: wrap;
    .content{
        width: calc(100% - 3rem);
        height: 100%;
        overflow-y: scroll;
        transition: all;
        transition-duration: .3s;
        .headStyle{
            display: none;
        }
        .el-backtop{
            background-color: #45a2ff;
        }
        .el-backtop, .el-calendar-table td.is-today{
            color: #fff;
        }
        .addFile{
            display: flex;
            flex-wrap: wrap;
            position: fixed;
            right: 0.3rem;
            bottom: 0.2rem;
            width: 0.55rem;
            z-index: 9;
            .el-icon-plus{
                width: 0.55rem;
                height: 0.55rem;
                line-height: 0.55rem;
                border-radius: 50%;
                background: #ff726f;
                box-shadow: 0 2px 3px rgba(0,0,0,0.15);
                display: inline-block;
                text-align: center;
                border: 0;
                padding: 0;
                color: #fff;
                font-size: 0.2rem;
                font-weight: bold;
                cursor: pointer;
                transition: all;
                transition-duration: .3s;
            }
            .el-icon-plus-new{
                background-color: #ff403c;
                transform: rotate(45deg);
            }
            .el-row /deep/{
                width: 100%;
                .el-col{
                    width: 100%;
                    display: flex;
                    justify-content: center;
                    i{
                        width: 0.4rem;
                        margin: 0 auto 0.15rem;
                        height: 0.4rem;
                        background-color: #ffc107;
                        border-radius: 50%;
                        text-align: center;
                        display: inline-block;
                        line-height: 40px;
                        box-shadow: 0 2px 3px rgba(0,0,0,0.15);
                        transform: scale(0);
                        position: relative;
                        animation-name: feba-btn-anim;
                        animation-duration: .3s;
                        animation-fill-mode: forwards;
                        color: #fff;
                        cursor: pointer;
                        font-size: 0.18rem;
                    }
                }
                @-webkit-keyframes feba-btn-anim {
                    from {
                        transform: scale(0);
                        opacity: 0;
                    }
                    to {
                        transform: scale(1);
                        opacity: 1;
                    }
                }

                @keyframes feba-btn-anim {
                    from {
                        transform: scale(0);
                        opacity: 0;
                    }
                    to {
                        transform: scale(1);
                        opacity: 1;
                    }
                }
            }
        }
        &::-webkit-scrollbar{
            width: 1px;
            height: 1px;
            background-color: #F5F5F5;
        }

        /*定义滚动条轨道 内阴影+圆角*/
        &::-webkit-scrollbar-track {
            box-shadow: inset 0 0 6px rgba(0, 0, 0, 0.3);
            -webkit-box-shadow: inset 0 0 6px rgba(0, 0, 0, 0.3);
            border-radius: 10px;
            background-color: #F5F5F5;
        }

        /*定义滑块 内阴影+圆角*/
        &::-webkit-scrollbar-thumb{
            border-radius: 10px;
            box-shadow: inset 0 0 6px rgba(0, 0, 0, .1);
            -webkit-box-shadow: inset 0 0 6px rgba(0, 0, 0, .1);
            background-color: #c8c8c8;
        }
        .progressStyle{
          position: fixed;
          z-index: 999999;
          bottom: 0px;
          right: 50px;
          background: #00b7ff none repeat scroll 0% 0%;
          font-size: 14px;
          color: #fff;
          padding: 0.2rem 0.4rem;
          width: 360px;
          display: flex;
          flex-wrap: wrap;
          .el-progress /deep/{
            width: 100%;
            .el-progress-bar{
              width: 100%;
              .el-progress-bar__inner{
                display: none;
              }
            }
            .el-progress__text{
                display: none;
                opacity: 0;
            }
          }
          .speed{
            display: flex;
            justify-content: center;
            align-items: center;
            width: 100%;
            margin: 0.2rem 0 0;
          }
        }
    }
}
@media screen and (max-width:999px){
.wrapper{
    .content{
        width: 100%;
        padding-top: 0.55rem;
        .headStyle.el-row /deep/{
            display: block;
            background-color: #32393f;
            padding: 10px 12px 9px 12px;
            text-align: center;
            position: fixed;
            z-index: 21;
            box-shadow: 0 0 10px rgba(0, 0, 0, 0.3);
            left: 0;
            top: 0;
            width: 100%;
            .el-col{
                display: flex;
                img{
                    display: block;
                    height: 35px;
                    margin: auto;
                }
                .el-button{
                    display: block;
                    height: 45px;
                    min-width: 45px;
                    text-align: center;
                    border-radius: 50%;
                    padding: 0;
                    border: 0;
                    background: none;
                    color: #fff;
                    font-size: 21px;
                    line-height: 45px;
                    -webkit-transition: all;
                    transition: all;
                    -webkit-transition-duration: .3s;
                    transition-duration: .3s;
                    cursor: pointer;
                }
            }
        }

    }
}
}
@media screen and (max-width:600px){
.wrapper{
    .el-dialog__wrapper /deep/{
        .el-dialog.customStyle{
            width: 300px;
        }
    }
}
}
</style>

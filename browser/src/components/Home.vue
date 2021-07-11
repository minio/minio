<template>
    <div class="wrapper" @click.stop="wrapperClick">
        <v-slide :class="{'sliMobile': slideShow}"
            :minioListBuckets="minioListBuckets" :currentBucket="currentBucket"
            :homeClick="homeClick" @homeClickFun="homeClickFun"
            @getminioListBucket="getminioListBucket" @getListBuckets="getListBuckets"></v-slide>
        <div class="content">
            <el-row class="headStyle">
                <el-col :span="6">
                    <el-button class="iconfont icon-ziyuan" @click="slideBtn" v-if="!slideShow"></el-button>
                    <el-button class="el-icon-back" style="background-color: #484b4e;" @click="slideBtn" v-else></el-button>
                </el-col>
                <el-col :span="12">
                    <img :src="logo" />
                </el-col>
                <el-col :span="6"></el-col>
            </el-row>
            <transition name="move" mode="out-in">
                <router-view
                :aboutServer="aboutServer" :aboutListObjects="aboutListObjects"
                :dialogFormVisible="dialogFormVisible" :currentBucket="currentBucket" :userd="userd"
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
                        <!--input type="file" ref="clearFile"
                        @change="getFile($event)" multiple="multiplt"
                        class="add-file-right-input" -->
                    </el-col>
                    <el-col :span="24">
                        <el-tooltip class="item" effect="dark" content="Create bucket" placement="left" @click.native="dialogFormVisible = true">
                            <i class="iconfont icon-harddriveyingpan"></i>
                        </el-tooltip>
                    </el-col>
                </el-row>
                <i class="el-icon-plus" :class="{'el-icon-plus-new': addFileShow}" @click.stop="addToggle"></i>
            </div>
            <el-backtop target=".wrapper"></el-backtop>
        </div>
    </div>
</template>

<script>
import axios from 'axios'
import vSlide from './Slide.vue';
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
            addArr: []
        }
    },
    components: {
        vSlide
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
        getFile(event){
           var file = event.target.files;
           for(var i = 0;i<file.length;i++){
                //    上传类型判断
                var imgName = file[i].name;
                var idx = imgName.lastIndexOf(".");
                if (idx != -1){
                    var ext = imgName.substr(idx+1).toUpperCase();
                    ext = ext.toLowerCase( );
                    /*if (ext!='pdf' && ext!='doc' && ext!='docx'){
                          console.log('upload type', ext);
                    }else{
                          this.addArr.push(file[i]);
                    }*/
                    this.addArr.push(file[i]);
                    this.submitAddFile(file[i])
                }else{

                }
           }
        },
        submitAddFile(cont){
           console.log(cont);
           let _this = this
           let $hgh
            if(0 == _this.addArr.length){
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
              let postUrl = _this.data_api + '/minio/upload/' + _this.currentBucket + '/' + cont.name
              axios.put(postUrl, {}, {headers: {
                  'Authorization':"Bearer "+ _this.$store.getters.accessToken,
                  'Content-Type': cont.type
              }}).then((response) => {
                  let json = response.data
                  console.log(json)

              }).catch(function (error) {
                  console.log(error);
                  // console.log(error.message, error.request, error.response.headers);
              });

              _this.getListObjects(_this.currentBucket, _this.prefixData)
            }

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
                _this.currentBucket = _this.minioListBuckets.buckets[0]?_this.minioListBuckets.buckets[0].name:''

                if(name) {
                  _this.getListObjects(name)
                  return false
                }
                _this.getListObjects()
                console.log('minioListBuckets home', _this.minioListBuckets)

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
                console.log(json, 'userd:', _this.userd)

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
                console.log(json)

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
                    bucketName: listName?listName:_this.minioListBuckets.buckets[0]?_this.minioListBuckets.buckets[0].name:'',
                    prefix: _this.prefixData?_this.prefixData:""
                }
            }
            _this.currentBucket = listName?listName:_this.minioListBuckets.buckets[0]?_this.minioListBuckets.buckets[0].name:''
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
                console.log(json)

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
            });
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
                      console.log('error', oldName)
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
        if(_this.minioListBuckets.buckets.length < 1){
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
          let postUrl = _this.data_api + '/minio/upload/' + _this.currentBucket + '/' + file.name
          axios.put(postUrl, {}, {headers: {
              'Authorization':"Bearer "+ _this.$store.getters.accessToken,
              'Content-Type': file.raw.type
          }}).then((response) => {
              let json = response.data
              console.log(json)

          }).catch(function (error) {
              console.log(error);
              // console.log(error.message, error.request, error.response.headers);
          });

          _this.getListObjects(_this.currentBucket, _this.prefixData)
        }
    },


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

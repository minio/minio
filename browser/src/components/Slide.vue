<template>
    <div class="slide" @click="caozuoFun()">
        <div class="fes-header">
            <img :src="logo" alt="">
            <h2>FS3 Browser</h2>
        </div>
        <div class="fes-search">
            <el-input
                placeholder="Search Buckets..."
                prefix-icon="el-icon-search"
                v-model="search"
                @input="searchBucketFun">
            </el-input>
            <el-row>
                <el-col :span="24" v-for="(item, index) in minioListBucketsAll.buckets" :key="index" :class="{'active': item.name == currentBucket}" @click.native="getListBucket(item.name)">
                    <div>
                        <i class="iconfont icon-harddriveyingpan"></i>
                        {{item.name}}
                    </div>
                    <i class="caozuo el-icon-more" @click.stop="caozuoFun(index, item.name)"></i>

                    <ul v-if="item.show && homeClick">
                        <li @click.stop="dialogFun(item.name, index)">Edit policy</li>
                        <li @click="backupFun">Backup to Filecoin</li>
                        <li @click.stop="dialogDeleteFun(item.name, index)">Delete</li>
                    </ul>
                </el-col>
            </el-row>
        </div>
        <div class="fes-host">
            <i class="iconfont icon-diqiu"></i>
            <a href="/">{{location}}</a>
        </div>

        <el-dialog :title="titlePolicy" :visible.sync="dialogFormVisible" custom-class="policyStyle">
            <el-form ref="dynamicValidateForm" class="demo-dynamic">
                <el-form-item>
                    <el-input v-model="dynamicValidateForm.value" placeholder="Prefix"></el-input>
                    <el-select v-model="dynamicValidateForm.valueSelect" placeholder="">
                        <el-option
                        v-for="item in dynamicValidateForm.options"
                        :key="item.value"
                        :label="item.label"
                        :value="item.value">
                        </el-option>
                    </el-select>
                    <el-button type="primary" @click="addPolicies">Add</el-button>
                </el-form-item>
                <el-form-item v-for="(domain, index) in bucketPolicies.policies" :key="index">
                    <!--el-input v-model="domain.prefix" placeholder="Prefix" disabled></el-input-->
                    <div class="el-input">
                      <div class="el-input__inner">{{domain.prefix?domain.prefix:'*'}}</div>
                    </div>
                    <el-select v-model="domain.policy" placeholder="" disabled>
                        <el-option
                        v-for="item in dynamicValidateForm.options"
                        :key="item.value"
                        :label="item.label"
                        :value="item.value">
                        </el-option>
                    </el-select>
                    <el-button type="danger" @click.prevent="removePolicies(domain)">Remove</el-button>
                </el-form-item>
            </el-form>
        </el-dialog>


        <!--share-dialog
          :shareDialog="shareDialog" :shareObjectShow="shareObjectShow"
          :shareFileShow="shareFileShow" @getshareDialog="getshareDialog">
        </share-dialog-->
    </div>
</template>
<script>
import axios from 'axios'
import shareDialog from '@/components/shareDialog.vue';
export default {
    data() {
        return {
            postUrl: this.data_api + `/minio/webrpc`,
            logo: require("@/assets/images/title.svg"),
            activeIndex: '1',
            mobileMenuShow: false,
            search: '',
            location: window.location.host,
            minioListBucketsAll: {},
            dialogFormVisible: false,
            titlePolicy: 'Bucket Policy ',
            dynamicValidateForm: {
                value: '',
                options: [{
                    value: 'readonly',
                    label: 'Read Only'
                },{
                    value: 'writeonly',
                    label: 'Write Only'
                },{
                    value: 'readwrite',
                    label: 'Read and Write'
                }],
                valueSelect: 'readonly'
            },
            bucketPolicies: {
                policies: []
            },
            shareDialog: false,
            shareObjectShow: true,
            shareFileShow: false,
        };
    },
    props: ['minioListBuckets', 'currentBucket', 'homeClick'],
    components: {
        shareDialog
    },
    computed: {
        email() {
            return this.$store.state.user.email
        },
    },
    watch: {
        $route: function (to, from) {
            if(this.bodyWidth){
                this.collapse = true
                this.collapseChage();
            }
        },
        'minioListBuckets': function (to, from) {
            this.getMinioData()
        },
        'homeClick': function (to, from) {
           console.log('homeClick change:', this.homeClick);
        },
    },
    methods: {
      getshareDialog(shareDialog) {
        this.shareDialog = shareDialog
      },
      backupFun() {
        this.shareDialog = true
        this.shareObjectShow = false
        this.shareFileShow = true
        this.$emit('getshareHome', true, false, true);
      },
      removePolicies(content) {
        let _this = this
        console.log('content', content)
        let dataSetBucketPolicy = {
                id: 1,
                jsonrpc: "2.0",
                method: "web.SetBucketPolicy",
                params:{
                    bucketName: content.bucket,
                    policy: "none",
                    prefix: content.prefix
                }
            }
            axios.post(_this.postUrl, dataSetBucketPolicy, {headers: {
                'Authorization':"Bearer "+ _this.$store.getters.accessToken
            }}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    return false
                }
                _this.dynamicValidateForm.value = ''
                _this.getListAllBucketPolicies(_this.currentBucket)

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
                _this.loginLoad=false
            });
      },
      addPolicies() {
            let _this = this
            let $hgh
            if(_this.bucketPolicies.policies) {
              _this.bucketPolicies.policies.map(item => {
                  if((item.prefix == _this.dynamicValidateForm.value && item.policy == _this.dynamicValidateForm.valueSelect) || ((!item.prefix) && _this.dynamicValidateForm.value == '*') && item.policy !== _this.dynamicValidateForm.valueSelect){
                      _this.$message({
                          message: 'Policy for this prefix already exists.',
                          type: 'warning',
                          showClose: true
                      });
                      $hgh = true
                      return false
                  }else{
                      console.log(item.prefix, _this.dynamicValidateForm.value, item.policy, _this.dynamicValidateForm.valueSelect);
                  }
              })
           }

           if(!$hgh){
            console.log('$hgh', 111);

            _this.setPolicyChange()

            setTimeout(function(){
              console.log('return ceshi111', _this.bucketPolicies.policies);
            },1000);
           }else{
            console.log('$hgh', 222);
           }

      },
      setPolicyChange() {
            let _this = this
            let dataSetBucketPolicy = {
                id: 1,
                jsonrpc: "2.0",
                method: "web.SetBucketPolicy",
                params:{
                    bucketName: _this.currentBucket,
                    policy: _this.dynamicValidateForm.valueSelect,
                    prefix: _this.dynamicValidateForm.value
                }
            }
            axios.post(_this.postUrl, dataSetBucketPolicy, {headers: {
                'Authorization':"Bearer "+ _this.$store.getters.accessToken
            }}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    return false
                }
                _this.dynamicValidateForm.value = ''
                _this.getListAllBucketPolicies(_this.currentBucket)

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
                _this.loginLoad=false
            });
      },
      dialogFun(name, index) {
        let _this = this
        _this.titlePolicy = 'Bucket Policy (' + name + ')'
        _this.dialogFormVisible = true
        if(_this.minioListBucketsAll) {
          _this.minioListBucketsAll.buckets.map((item, i) => {
              item.show = false;
          })
        }

        _this.getListAllBucketPolicies(name)
      },
      dialogDeleteFun(name, index) {
        let _this = this
        let dataDeleteBucket = {
            id: 1,
            jsonrpc: "2.0",
            method: "web.DeleteBucket",
            params:{
                bucketName: name,
            }
        }
        axios.post(_this.postUrl, dataDeleteBucket, {headers: {
            'Authorization':"Bearer "+ _this.$store.getters.accessToken
        }}).then((response) => {
            let json = response.data
            let error = json.error
            let result = json.result
            if (error) {
                _this.$message.error(error.message);
                return false
            }

            _this.$emit('getListBuckets');
            console.log(json)

        }).catch(function (error) {
            console.log(error);
            // console.log(error.message, error.request, error.response.headers);
            _this.loginLoad=false
        });
      },
      getListAllBucketPolicies(name) {
        let _this = this
        let dataListAllBucketPolicies = {
            id: 1,
            jsonrpc: "2.0",
            method: "web.ListAllBucketPolicies",
            params:{
                bucketName: name,
            }
        }
        axios.post(_this.postUrl, dataListAllBucketPolicies, {headers: {
            'Authorization':"Bearer "+ _this.$store.getters.accessToken
        }}).then((response) => {
            let json = response.data
            let error = json.error
            let result = json.result
            if (error) {
                _this.$message.error(error.message);
                return false
            }
            _this.bucketPolicies = result
            /*if(_this.bucketPolicies.policies) {
              _this.bucketPolicies.policies.map(item => {
                  if(!item.prefix){
                      item.prefix = '*'
                  }
              })
             }*/

        }).catch(function (error) {
            console.log(error);
            // console.log(error.message, error.request, error.response.headers);
            _this.loginLoad=false
        });
      },
      handleSelect(key, keyPath) {
        console.log(key, keyPath);
      },
      mobileMenuFun(){
        let _this = this;
        _this.mobileMenuShow=!_this.mobileMenuShow;
        if(_this.mobileMenuShow){
          document.body.style.height = '100vh'
          document.body.style['overflow-y'] = 'hidden'
        }else{
          document.body.style.height = 'auto'
          document.body.style['overflow-y'] = 'auto'
        }
      },
      caozuoFun(index, name) {
        let _this = this;
        _this.$nextTick(() => {
          if(_this.minioListBucketsAll.buckets) {
            _this.minioListBucketsAll.buckets.map((item, i) => {
                item.show = false;
                if(i == index){
                    item.show = true
                    _this.$emit('homeClickFun', true)
                }
            })
           }
        })
        if(name){
            _this.getListBucket(name)
        }
      },
      searchBucketFun() {
          let _this = this
          if(_this.search){
            _this.minioListBucketsAll.buckets = []
            if(_this.minioListBuckets.buckets) {
              _this.minioListBuckets.buckets.map(item => {
                  if(item.name.indexOf(_this.search) >= 0){
                      _this.minioListBucketsAll.buckets.push(item)
                  }
              })
            }
          }else{
              _this.minioListBucketsAll = JSON.parse(JSON.stringify(_this.minioListBuckets))
          }
      },
      getListBucket(name) {
          this.$emit('getminioListBucket', name);
      },
      getMinioData() {
        let _this = this;
        if(_this.minioListBuckets && _this.minioListBuckets.buckets){
            _this.minioListBuckets.buckets.map(item => {
                item.show = false;
            })
            _this.minioListBucketsAll = JSON.parse(JSON.stringify(_this.minioListBuckets))

            console.log('minioListBucketsAll', _this.minioListBucketsAll);
        }else{
          _this.minioListBucketsAll = JSON.parse(JSON.stringify(_this.minioListBuckets))
        }
      }
    },
    mounted() {
      this.getMinioData()
    },
};
</script>
<style lang="scss" scoped>
.slide{
    width: 2.5rem;
    background-color: #00303f;
    height: calc(100% - 0.5rem);
    overflow: hidden;
    padding: 0.25rem;
    transition: all;
    transition-duration: .3s;
    .fes-header{
        display: flex;
        width: 100%;
        margin-bottom: 40px;
        img{
            width: 20px;
            margin-top: 5px;
        }
        h2{
            margin: 10px 0 0 13px;
            font-weight: 400;
            color: #fff;
            font-size: 0.2rem;
        }
    }
    .fes-search{
        height: calc(100% - 1.5rem);
        .el-input /deep/{
            display: block;
            clear: both;
            .el-input__inner{
                background-color: transparent;
                box-shadow: none;
                border: 0;
                border-radius: 0;
                border-bottom: 1px solid rgba(255, 255, 255, 0.1);
                color: #fff;
                text-align: left;
                font-size: 0.13rem;
            }
        }
        .el-row /deep/{
            margin-top: 0.2rem;
            margin-left: -0.25rem;
            margin-right: -0.25rem;
            font-size: 0.13rem;
            height: calc(100% - 0.6rem);
            overflow: hidden;
            overflow-y: scroll;
            .el-col{
                position: relative;
                display: flex;
                align-items: center;
                justify-content: space-between;
                padding: 0.1rem 0.05rem 0.1rem 0.25rem;
                color: rgba(255, 255, 255, 0.75);
                word-wrap: break-word;
                font-size: 0.14rem;
                cursor: pointer;
                div{
                    display: flex;
                    align-items: center;
                }
                i{
                    font-size: 0.18rem;
                    margin-right: 0.08rem;
                    color: rgba(255, 255, 255, 0.75);
                }
                .caozuo{
                    opacity: 0;
                    float: right;
                    transform: rotate(90deg);
                    font-size: 0.15rem;
                    color: rgba(255, 255, 255, 0.75);
                    &:hover{
                        color: #fff;
                    }
                }
                ul{
                    position: absolute;
                    right: 0;
                    top: 0;
                    padding: 0.15rem 0;
                    background-color: #fff;
                    border-radius: 0.05rem;
                    z-index: 1000;
                    min-width: 160px;
                    margin: 2px 0 0;
                    list-style: none;
                    font-size: 0.15rem;
                    text-align: left;
                    border: 1px solid transparent;
                    border-radius: 4px;
                    box-shadow: 0 6px 12px rgba(0,0,0,.18);
                    background-clip: padding-box;
                    li{
                        display: block;
                        padding: 0.08rem 0.1rem;
                        clear: both;
                        font-weight: 400;
                        line-height: 1.42857143;
                        color: #8e8e8e;
                        white-space: nowrap;
                        text-align: right;
                        &:hover{
                            text-decoration: none;
                            color: #333;
                            background-color: rgba(0,0,0,.05);
                        }
                    }
                }
                &:hover{
                    background: rgba(0,0,0,.1);
                    .caozuo{
                        opacity: 1;
                    }
                }
            }
            .active{
                background: rgba(0,0,0,.1);
                font-size: 0.15rem;
                color: #fff;
                i{
                    color: #fff;
                }
            }
        }
        .el-row{
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
    .fes-host {
        position: fixed;
        left: 0;
        bottom: 0;
        z-index: 21;
        background-color: rgba(0,0,0,.1);
        font-size: 15px;
        font-weight: 400;
        width: calc(3rem - 0.4rem);
        padding: 0.2rem;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        color: hsla(0,0%,100%,.75);
        transition: all;
        transition-duration: .3s;
        i{
            float: left;
            display: flex;
            justify-content: center;
            align-items: center;
            font-size: 0.2rem;
            margin-right: 10px;
            width: 20px;
            height: 20px;
            color: #888b83;
            // background: url(../assets/images/icon_01.jpg) no-repeat center;
            // background-size: 100%;
        }
        a{
            color: hsla(0,0%,100%,.75);
            font-size: 15px;
            font-weight: 400;
        }
    }
    .el-dialog__wrapper /deep/{
        .policyStyle{
            width: 90%;
            max-width: 600px;
            .el-dialog__header{
                display: flex;
                .el-dialog__title{
                    font-size: 0.15rem;
                    color: #333;
                }
            }
            .el-dialog__body{
                padding: 0.3rem 0;
                .el-form-item{
                    padding: 0.1rem 0.3rem;
                    margin-bottom: 0.1rem;
                    &:nth-child(2n+2){
                        background-color: #f7f7f7;
                    }
                }
                .el-form-item__content{
                    display: flex;
                    line-height: 0.3rem;
                    .el-input{
                        .el-input__inner{
                            height: 0.3rem;
                            padding-left: 0;
                            line-height: 0.3rem;
                            border: 0;
                            border-bottom: 1px solid #f7f7f7;
                            background-color: transparent;
                            border-radius: 0;
                            font-size: 0.13rem;
                            color: #32393f;
                            text-align: left;
                        }
                        .el-input__icon{
                            display: flex;
                            align-items: center;
                        }
                    }
                    .el-input.is-disabled{
                        .el-input__inner{
                            background-color: transparent;
                        }
                    }
                    .el-select{
                        margin: 0 5%;
                    }
                    .el-button{
                        width: 130px;
                        height: 0.3rem;
                        padding: 0;
                        line-height: 0.3rem;
                        color: #fff;
                        font-size: 12px;
                        border: 0;
                        border-radius: 0.02rem;
                        text-align: center;
                    }
                }
            }
        }
    }
}
.sliMobile{
    transform: translate3d(0,0,0) !important;
}

@media screen and (max-width:1024px){

}
@media screen and (max-width: 999px){
.slide{
    position: fixed;
    left: 0;
    top: 0;
    z-index: 20;
    transform: translate3d(-3rem,0,0);
    .fes-search {
      .el-row /deep/{
        .el-col{
          .caozuo{
            opacity: 1;
          }
        }
      }
    }
}
}
</style>

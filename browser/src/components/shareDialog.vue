<template>
      <el-dialog title="" :visible.sync="shareDialog" :custom-class="{'ShareObjectMobile': shareFileShowMobile, 'ShareObject': 1 === 1}" :before-close="getDiglogChange">
          <div class="shareContent">
              <el-row class="share_left" v-if="shareObjectShow">
                <!--el-button class="shareFileCoin" @click="shareFileShowFun">Share to Filecoin >></el-button-->
                <el-col :span="24">
                  <h4>Share Object</h4>
                </el-col>
                <el-col :span="24">
                  <h5>Shareable Link</h5>
                  <el-input v-model="share_input" placeholder="" id="url-link" disabled></el-input>
                </el-col>
                <el-col :span="24">
                  <h5>Expires in (Max 7 days)</h5>
                  <el-row class="steppet">
                    <el-col :span="8" v-if="num">
                      <div class="set-expire-title">Days</div>
                      <el-input-number v-model="num.num_Day" @change="handleChange" :min="1" :max="7" label="Days"></el-input-number>
                    </el-col>
                    <el-col :span="8" v-if="num">
                      <div class="set-expire-title">Hours</div>
                      <el-input-number v-model="num.num_Hours" @change="handleChange" :min="0" :max="23" label="Hours"></el-input-number>
                    </el-col>
                    <el-col :span="8" v-if="num">
                      <div class="set-expire-title">Minutes</div>
                      <el-input-number v-model="num.num_Minutes" @change="handleChange" :min="0" :max="59" label="Minutes"></el-input-number>
                    </el-col>
                  </el-row>
                </el-col>
                <el-col :span="24">
                  <div class="btncompose">
                    <el-button @click="copyLink(share_input)">Copy Link</el-button>
                    <el-button @click="getDiglogChange">Cancel</el-button>
                  </div>
                </el-col>
              </el-row>


              <el-row class="share_right" v-if="shareFileShow">
                <el-button class="shareFileCoinSend" @click="submitForm('ruleForm')">Send</el-button>
                <el-col :span="24">
                  <!--h4 v-if="shareObjectShow">Share to Filecoin</h4-->
                  <h4>Backup to Filecoin</h4>
                </el-col>
                <el-col :span="24">
                  <el-form :model="ruleForm" :rules="rules" ref="ruleForm" label-width="110px" class="demo-ruleForm">
                    <el-form-item label="Miner ID:" prop="minerId">
                      <el-input v-model="ruleForm.minerId"></el-input>
                    </el-form-item>
                    <el-form-item label="Price:" prop="price">
                      <el-input v-model="ruleForm.price" onkeyup="value=value.replace(/^\D*(\d*(?:\.\d{0,20})?).*$/g, '$1')"></el-input> FIL
                    </el-form-item>
                    <el-form-item label="Duration:" prop="duration">
                      <el-input v-model="ruleForm.duration" onkeyup="value=value.replace(/^(0+)|[^\d]+/g,'')"></el-input> Day
                    </el-form-item>
                    <el-form-item label="Verified-Deal:" prop="verified">
                      <el-radio v-model="ruleForm.verified" label="1">True</el-radio>
                      <el-radio v-model="ruleForm.verified" label="2">False</el-radio>
                    </el-form-item>
                    <el-form-item label="Fast-Retrival:" prop="fastRetirval">
                      <el-radio v-model="ruleForm.fastRetirval" label="1">True</el-radio>
                      <el-radio v-model="ruleForm.fastRetirval" label="2">False</el-radio>
                    </el-form-item>
                  </el-form>
                </el-col>
                <el-col :span="24">
                   <h4 style="margin: 0;">Deal CID <i class="el-icon-document-copy" v-if="ruleForm.dealCID" @click="copyLink(ruleForm.dealCID)"></i></h4>
                 </el-col>
                 <el-col :span="24">
                   <el-input
                     type="textarea"
                     :rows="4"
                     placeholder=""
                     v-model="ruleForm.dealCID"
                     disabled>
                   </el-input>
                 </el-col>
                <!--el-col :span="24">
                  <h4 style="margin: 0;">Retrieval from Filecoin Network <i class="el-icon-document-copy"></i></h4>
                </el-col>
                <el-col :span="24">
                  <el-input
                    type="textarea"
                    :rows="4"
                    placeholder=""
                    v-model="ruleForm.textarea"
                    disabled>
                  </el-input>
                </el-col-->
              </el-row>
          </div>
      </el-dialog>

</template>

<script>
import axios from 'axios'
export default {
    data() {
        return {
            postUrl: this.data_api + `/minio/webrpc`,
            shareFileShowMobile: false,
            ruleForm: {
              minerId: '',
              price: '',
              duration: '',
              verified: '2',
              fastRetirval: '1',
              textarea: 'lotus client',
              dealCID: ''
            },
            rules: {
               minerId: [
                 { required: true, message: 'Please enter Miner ID', trigger: 'blur' }
               ],
               price: [
                 { required: true, message: 'Please enter Price', trigger: 'blur' }
               ],
               duration: [
                 { required: true, message: 'Please enter Duration', trigger: 'blur' }
               ],
            },
        }
    },
    props: ['shareDialog','shareObjectShow','shareFileShow', 'num', 'share_input', 'postAdress'],
    watch: {
      'shareDialog': function(){
        let _this = this
        if(!_this.shareDialog){
          _this.ruleForm = {
              minerId: '',
              price: '',
              duration: '',
              verified: '2',
              fastRetirval: '1',
              dealCID: ''
          }
        }
      }
    },
    methods: {
      copyDealCid() {

      },
      shareFileShowFun() {
        this.shareFileShow = !this.shareFileShow
        this.shareFileShowMobile = !this.shareFileShowMobile
      },
      submitForm(formName) {
        this.$refs[formName].validate((valid) => {
          if (valid) {

            let _this = this
            let postUrl01 = _this.data_api + `/minio/deal/` + _this.postAdress
            let postUrl = `http://192.168.88.41:9000/minio/deal/` + _this.postAdress
            let minioDeal = {
                "VerifiedDeal": _this.ruleForm.verified == '2'? 'false' : 'true',
                "FastRetrieval": _this.ruleForm.fastRetirval == '2'? 'false' : 'true',
                "MinerId": _this.ruleForm.minerId,
                "Price": _this.ruleForm.price,
                "Duration": _this.ruleForm.duration*24*60*2   //（UI上用户输入天数，需要转化成epoch给后端。例如10天, 就是 10*24*60*2）
            }

            axios.post(postUrl, minioDeal, {}).then((response) => {
                let json = response.data
                let error = json.error
                let result = json.result
                if (error) {
                    _this.$message.error(error.message);
                    return false
                }

                _this.ruleForm.dealCID = json.dealCid
                _this.$message({
                  message: 'Transaction has been successfully sent.',
                  type: 'success'
                });

            }).catch(function (error) {
                console.log(error);
                // console.log(error.message, error.request, error.response.headers);
            });

          } else {
            console.log('error submit!!');
            return false;
          }
        });
      },
      getDiglogChange() {
        this.$emit('getshareDialog', false)
      },
      handleChange(value) {
          console.log(this.num)
          this.$emit('getShareGet', this.num)
      },
      copyLink(text){
        let _this = this
        var txtArea = document.createElement("textarea");
        txtArea.id = 'txt';
        txtArea.style.position = 'fixed';
        txtArea.style.top = '0';
        txtArea.style.left = '0';
        txtArea.style.opacity = '0';
        txtArea.value = text;
        document.body.appendChild(txtArea);
        txtArea.select();

        try {
            var successful = document.execCommand('copy');
            var msg = successful ? 'Link copied to clipboard!' : 'copy failed!';
            console.log('Copying text command was ' + msg);
            if (successful) {
                _this.$message({
                    message: msg,
                    type: 'success'
                });
            }
        } catch (err) {
            console.log('Oops, unable to copy');
        } finally {
            document.body.removeChild(txtArea);
        }
      },

    },
    mounted() {
    },
};
</script>

<style lang="scss" scoped>
.el-dialog__wrapper /deep/{
  justify-content: center;
  display: flex;
  align-items: center;
  .ShareObject{
        position: relative;
        width: auto;
        .shareFileCoin, .shareFileCoinSend{
            position: absolute;
            top: .14rem;
            right: 0.15rem;
            padding: .05rem .1rem;
            font-size: 0.14rem;
            color: #4070ff;
            border: 0;
            background-color: #fff;
            line-height: 1.5;
            border-radius: 2px;
            text-align: center;
            transition: all;
            transition-duration: 0s;
            transition-duration: .3s;
            font-weight: normal;
            text-decoration: underline;
        }
        .shareFileCoinSend{
            color: #fff;
            background-color: #33d46f;
            text-decoration: none;
        }
        .el-dialog__header{
            display: none;
            .el-dialog__title{
                font-size: 0.15rem;
                color: #333;
            }
        }
        .el-dialog__body{
          padding: 0;
          .shareContent{
            display: flex;
            //align-items: center;
            justify-content: center;
            flex-wrap: wrap;
            width: 100%;
            .el-row{
              position: relative;
              width: 400px;
              .el-col{
                padding: 0 0.2rem;
                margin-bottom: 0.25rem;
                h4{
                  font-weight: normal;
                  display: block;
                  margin: 20px 0 10px;;
                  line-height: 2;
                  font-size: .15rem;
                  color: #333;
                }
                h5{
                  font-size: 13px;
                  font-weight: normal;
                  display: block;
                  margin-bottom: 10px;
                  line-height: 2;
                  color: #8e8e8e;
                }
                .el-input{
                  .el-input__inner{
                    padding: 0.1rem;
                    border: 1px solid #eee;
                    border-radius: 0.02rem;
                    font-size: 13px;
                    cursor: text;
                    transition: border-color;
                    transition-duration: .3s;
                    background-color: transparent;
                  }
                }
                .steppet{
                  display: flex;
                  justify-content: center;
                  width: 100%;
                  .el-col{
                    position: relative;
                    margin: 0;
                    .set-expire-title {
                      position: absolute;
                      top: 40px;
                      left: 0;
                      right: 0;
                      font-size: 10px;
                      text-transform: uppercase;
                      text-align: center;
                      line-height: 1.42857143;
                      color: #8e8e8e;
                    }
                    .el-input-number{
                      display: flex;
                      flex-wrap: wrap;
                      width: 100%;
                      height: 125px;
                      span, .el-input{
                        width: 100%;
                      }
                      .el-input-number__decrease{
                        background: transparent url(../assets/images/down.png) no-repeat center;
                        background-size: auto 100%;
                        height: 20px;
                        top: auto;
                        bottom: 0;
                        border: 0;
                        i{
                          display: none;
                        }
                      }
                      .el-input-number__increase{
                        bottom: auto;
                        background: transparent url(../assets/images/up.png) no-repeat center;
                        background-size: auto 100%;
                        height: 20px;
                        top: 0;
                        border: 0;
                        i{
                          display: none;
                        }
                      }
                      .el-input{
                        position: absolute;
                        top: 27px;
                        bottom: 27px;
                        border: 1px solid #eee;
                        pointer-events: none;
                        .el-input__inner{
                          position: absolute;
                          bottom: 0;
                          border: 0;
                          background-color: transparent;
                          box-shadow: none;
                          color: #333;
                          font-size: 0.2rem;
                          font-weight: 400;
                        }
                      }
                    }
                  }
                }
                .btncompose{
                  display: flex;
                  align-items: center;
                  justify-content: center;
                  .el-button{
                    padding: 0.05rem 0.1rem;
                    margin: 0 0.03rem;
                    font-size: 12px;
                    color: #fff;
                    border: 0;
                    background-color: #33d46f;
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
            .share_right{
              padding: 0.05rem 0 .2rem;
              .el-col{
                display: flex;
                align-items: center;
                margin-bottom: 0.1rem;
                h4{
                  i{
                    margin: 0 0 0 5px;
                    cursor: pointer;
                  }
                }
                h5{
                  margin: 0 5px 0 0;
                  white-space: nowrap;
                }
                .el-input{
                  .el-input__inner{
                    padding: 0.1rem;
                    border: 1px solid #eee;
                    border-radius: 0.02rem;
                    font-size: 13px;
                    cursor: text;
                    transition: border-color;
                    transition-duration: .3s;
                    background-color: transparent;
                  }
                }
                .el-form{
                  width: 100%;
                  .el-form-item{
                    margin-bottom: 0.05rem;
                    .el-form-item__content{
                      .el-input{
                        width: calc(100% - 40px);
                      }
                    }
                  }
                  .el-form-item.is-error, .el-form-item.is-required{
                    margin-bottom: 0.15rem;
                  }
                }
              }
              &:after{
                content: '';
                position: absolute;
                left: 0;
                top: 0;
                bottom: 0;
                width: 1px;
                height: 100%;
                background-color: #eee;
              }
            }
          }
        }
      }
    }

@media screen and (max-width:769px){

  .el-dialog__wrapper /deep/{
    .ShareObject{
      .el-dialog__body{
        .shareContent{
          .el-row{
            width: 300px;
          }
        }
      }
    }
  }
}
@media screen and (max-width:600px){

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
    .ShareObjectMobile {
      margin-top: 55vh !important;
    }
  }
}
</style>

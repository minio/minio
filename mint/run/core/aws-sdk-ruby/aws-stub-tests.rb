#!/usr/bin/env ruby
#
#

require 'aws-sdk'
require 'securerandom'
require 'net/http'
require 'multipart_body'

# For aws-sdk ruby tests to run, setting the following
# environment variables is mandatory.
# SERVER_ENDPOINT: <ip:port> address of the minio server tests will run against
# ACCESS_KEY: access key for the minio server
# SECRET_KEY: secreet key for the minio server
# SERVER_REGION: region minio server is setup to run
# ENABLE_HTTPS: (1|0) turn on/off to specify https or
#                 http services minio server is running on
# MINT_DATA_DIR: Data directory where test data files are stored

class AwsSdkRubyTest
  # Set variables necessary to create an s3 client instance.
  # Get them from the environment variables

  # Region information, eg. "us-east-1"
  region = ENV['SERVER_REGION'] ||= 'SERVER_REGION is not set'
  # Minio server, eg. "play.minio.io:9000"
  access_key_id = ENV['ACCESS_KEY'] ||= 'ACCESS_KEY is not set'
  secret_access_key = ENV['SECRET_KEY'] ||= 'SECRET_KEY is not set'
  enable_https = ENV['ENABLE_HTTPS']
  end_point = ENV['SERVER_ENDPOINT'] ||= 'SERVER_ENDPOINT is not set'
  endpoint = enable_https == '1' ? 'https://' + end_point : 'http://' + end_point

  # Create s3 resource instance,"s3"
  @@s3 = Aws::S3::Resource.new(
    region: region,
    endpoint: endpoint,
    access_key_id: access_key_id,
    secret_access_key: secret_access_key,
    force_path_style: true)

  def initialize_log_output(meth, alert = nil)
    # Initialize and return log content in log_output hash table

    # Collect args in args_arr
    args_arr = method(meth).parameters.flatten.map(&:to_s)
                           .reject { |x| x == 'req' || x == 'opt' }
    # Create and return log output content
    { name: 'aws-sdk-ruby',
      function: "#{meth}(#{args_arr.join(',')})",  # method name and arguments
      args: args_arr,  # array of arg names. This'll be replaced with a
                       # a arg/value pairs insdie the caller method
      duration: 0,  # test runtime duration in seconds
      alert: alert,
      message: nil,
      error: nil }
  end

  def random_bucket_name
    'aws-sdk-ruby-bucket-' + SecureRandom.hex(6)
  end

  def calculate_duration(t2, t1)
    # Durations are in miliseconds, with precision of 2 decimal places
    ((t2 - t1) * 1000).round(2)
  end

  def print_log(log_output, start_time)
    # Calculate duration in miliseconds
    log_output[:duration] = calculate_duration(Time.now, start_time)
    # Get rid of the log_output fields if nil
    puts log_output.delete_if{|k, value| value == nil}.to_json
    # Exit at the first failure
    exit 1 if log_output[:status] == 'FAIL'
  end

  def cleanUp(buckets, log_output)
    # Removes objects and bucket if bucket exists
    bucket_name = ''
    buckets.each do |b|
      bucket_name = b
      if bucketExistsWrapper(b, log_output)
        removeObjectsWrapper(b, log_output)
        removeBucketWrapper(b, log_output)
      end
    end
  rescue => e
    raise "Failed to clean-up bucket '#{bucket_name}', #{e}"
  end

  #
  # API commands/methods
  #
  def makeBucket(bucket_name)
    # Creates a bucket, "bucket_name"
    # on S3 client , "s3".
    # Returns bucket_name if already exists
    @@s3.bucket(bucket_name).exists? ? @@s3.bucket(bucket_name) : @@s3.create_bucket(bucket: bucket_name)
  rescue => e
    raise e
  end

  def makeBucketWrapper(bucket_name, log_output)
    makeBucket(bucket_name)
  rescue => e
    log_output[:function] = "makeBucket(bucket_name)"
    log_output[:args] = {'bucket_name': bucket_name}
    raise e
  end

  def removeBucket(bucket_name)
    # Deletes/removes bucket, "bucket_name" on S3 client, "s3"
    @@s3.bucket(bucket_name).delete
  rescue => e
    raise e
  end

  def removeBucketWrapper(bucket_name, log_output)
    removeBucket(bucket_name)
  rescue => e
    log_output[:function] = "removeBucket(bucket_name)"
    log_output[:args] = {'bucket_name': bucket_name}
    raise e
  end

  def putObject(bucket_name, file)
    # Creates "file" (full path) in bucket, "bucket_name",
    # on S3 client, "s3"
    file_name = File.basename(file)
    @@s3.bucket(bucket_name).object(file_name).upload_file(file)
  rescue => e
    raise e
  end

  def putObjectWrapper(bucket_name, file, log_output)
    putObject(bucket_name, file)
  rescue => e
    log_output[:function] = "putObject(bucket_name, file)"
    log_output[:args] = {'bucket_name': bucket_name,
                         'file': file}
    raise e
  end

  def getObject(bucket_name, file, destination)
    # Gets/Downloads file, "file",
    # from bucket, "bucket_name", of S3 client, "s3"
    file_name = File.basename(file)
    dest = File.join(destination, file_name)
    @@s3.bucket(bucket_name).object(file_name).get(response_target: dest)
  rescue => e
    raise e
  end

  def getObjectWrapper(bucket_name, file, destination, log_output)
    getObject(bucket_name, file, destination)
  rescue => e
    log_output[:function] = "getObject(bucket_name, file)"
    log_output[:args] = {'bucket_name': bucket_name,
                         'file': file,
                         'destination': destination}
    raise e
  end

  def copyObject(source_bucket_name, target_bucket_name, source_file_name, target_file_name = '')
    # Copies file, "file_name", from source bucket,
    # "source_bucket_name", to target bucket,
    # "target_bucket_name", on S3 client, "s3"
    target_file_name = source_file_name if target_file_name.empty?
    source = @@s3.bucket(source_bucket_name)
    target = @@s3.bucket(target_bucket_name)
    source_obj = source.object(source_file_name)
    target_obj = target.object(target_file_name)
    source_obj.copy_to(target_obj)
  rescue => e
    raise e
  end

  def copyObjectWrapper(source_bucket_name, target_bucket_name, source_file_name, target_file_name = '', log_output)
    copyObject(source_bucket_name, target_bucket_name, source_file_name, target_file_name)
  rescue => e
    log_output[:function] = 'copyObject(source_bucket_name, target_bucket_name, source_file_name, target_file_name = '')'
    log_output[:args] = {'source_bucket_name': source_bucket_name,
                         'target_bucket_name': target_bucket_name,
                         'source_file_name': source_file_name,
                         'target_file_name': target_file_name}
    raise e
  end

  def removeObject(bucket_name, file)
    # Deletes file in bucket,
    # "bucket_name", on S3 client, "s3".
    # If file, "file_name" does not exist,
    # it quietly returns without any error message
    @@s3.bucket(bucket_name).object(file).delete
  rescue => e
    raise e
  end

  def removeObjectWrapper(bucket_name, file_name, log_output)
    removeObject(bucket_name, file_name)
  rescue => e
    log_output[:function] = "removeObject(bucket_name, file_name)"
    log_output[:args] = {'bucket_name': bucket_name,
                         'file_name': file_name}
    raise e
  end

  def removeObjects(bucket_name)
    # Deletes all files in bucket, "bucket_name"
    # on S3 client, "s3"
    file_name = ''
    @@s3.bucket(bucket_name).objects.each do |obj|
      file_name = obj.key
      obj.delete
    end
  rescue => e
    raise "File name: '#{file_name}', #{e}"
  end

  def removeObjectsWrapper(bucket_name, log_output)
    removeObjects(bucket_name)
  rescue => e
    log_output[:function] = 'removeObjects(bucket_name)'
    log_output[:args] = {'bucket_name': bucket_name}
    raise e
  end

  def listBuckets
    # Returns an array of bucket names on S3 client, "s3"
    bucket_name_list = []
    @@s3.buckets.each do |b|
      bucket_name_list.push(b.name)
    end
    return bucket_name_list
  rescue => e
    raise e
  end

  def listBucketsWrapper(log_output)
    listBuckets
  rescue => e
    log_output[:function] = 'listBuckets'
    log_output[:args] = {}
    raise e
  end

  def listObjects(bucket_name)
    # Returns an array of object/file names
    # in bucket, "bucket_name", on S3 client, "s3"
    object_list = []
    @@s3.bucket(bucket_name).objects.each do |obj|
      object_list.push(obj.key)
    end
    return object_list
  rescue => e
    raise e
  end

  def listObjectsWrapper(bucket_name, log_output)
    listObjects(bucket_name)
  rescue => e
    log_output[:function] = 'listObjects(bucket_name)'
    log_output[:args] = {'bucket_name': bucket_name}
    raise e
  end

  def statObject(bucket_name, file_name)
    return @@s3.bucket(bucket_name).object(file_name).exists?
  rescue => e
    raise e
  end

  def statObjectWrapper(bucket_name, file_name, log_output)
    statObject(bucket_name, file_name)
  rescue => e
    log_output[:function] = 'statObject(bucket_name, file_name)'
    log_output[:args] = {'bucket_name': bucket_name,
                         'file_name': file_name}
    raise e
  end

  def bucketExists?(bucket_name)
    # Returns true if bucket, "bucket_name", exists,
    # false otherwise
    return @@s3.bucket(bucket_name).exists?
  rescue => e
    raise e
  end

  def bucketExistsWrapper(bucket_name, log_output)
    bucketExists?(bucket_name)
  rescue => e
    log_output[:function] = 'bucketExists?(bucket_name)'
    log_output[:args] = {'bucket_name': bucket_name}
    raise e
  end

  def presignedGet(bucket_name, file_name)
    # Returns download/get url
    obj = @@s3.bucket(bucket_name).object(file_name)
    return obj.presigned_url(:get, expires_in: 600)
  rescue => e
    raise e
  end

  def presignedGetWrapper(bucket_name, file_name, log_output)
    presignedGet(bucket_name, file_name)
  rescue => e
    log_output[:function] = 'presignedGet(bucket_name, file_name)'
    log_output[:args] = {'bucket_name': bucket_name,
                         'file_name': file_name}
    raise e
  end

  def presignedPut(bucket_name, file_name)
    # Returns put url
    obj = @@s3.bucket(bucket_name).object(file_name)
    return obj.presigned_url(:put, expires_in: 600)
  rescue => e
    raise e
  end

  def presignedPutWrapper(bucket_name, file_name, log_output)
    presignedPut(bucket_name, file_name)
  rescue => e
    log_output[:function] = 'presignedPut(bucket_name, file_name)'
    log_output[:args] = {'bucket_name': bucket_name,
                         'file_name': file_name}
   raise e
  end

  def presignedPost(bucket_name, file_name, expires_in_sec, max_byte_size)
    # Returns upload/post url
    obj = @@s3.bucket(bucket_name).object(file_name)
    return obj.presigned_post(expires: Time.now + expires_in_sec,
                              content_length_range: 1..max_byte_size)
  rescue => e
    raise e
  end

  def presignedPostWrapper(bucket_name, file_name, expires_in_sec, max_byte_size, log_output)
    presignedPost(bucket_name, file_name, expires_in_sec, max_byte_size)
  rescue => e
    log_output[:function] = 'presignedPost(bucket_name, file_name, expires_in_sec, max_byte_size)'
    log_output[:args] = {'bucket_name': bucket_name,
                         'file_name': file_name,
                         'expires_in_sec': expires_in_sec,
                         'max_byte_size': max_byte_size}
    raise e
  end

  # To be addressed. S3 API 'get_bucket_policy' does not work!
  # def getBucketPolicy(bucket_name)
  #   # Returns bucket policy
  #   return @@s3.bucket(bucket_name).get_bucket_policy
  # rescue => e
  #   raise e
  # end

  #
  # Test case methods
  #
  def listBucketsTest
    # Tests listBuckets api command by creating
    # new buckets from bucket_name_list

    # get 2 different random bucket names and create a list
    bucket_name_list = [random_bucket_name, random_bucket_name]
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('listBuckets')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      prev_total_buckets = listBucketsWrapper(log_output).length
      new_buckets = bucket_name_list.length
      bucket_name_list.each do |b|
        makeBucketWrapper(b, log_output)
      end
      new_total_buckets = prev_total_buckets + new_buckets
      if new_total_buckets >= prev_total_buckets + new_buckets
        log_output[:status] = 'PASS'
      else
        log_output[:error] = 'Could not find expected number of buckets'
        log_output[:status] = 'FAIL'
      end
      cleanUp(bucket_name_list, log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def makeBucketTest
    # Tests makeBucket api command.

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('makeBucket')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)

      if bucketExistsWrapper(bucket_name, log_output)
        log_output[:status] = 'PASS'
      else
        log_output[:error] = 'Bucket expected to be created does not exist'
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def bucketExistsNegativeTest
    # Tests bucketExists api command.

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('bucketExists?')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      if !bucketExistsWrapper(bucket_name, log_output)
        log_output[:status] = 'PASS'
      else
        log_output[:error] = "Failed to return 'false' for a non-existing bucket"
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def removeBucketTest
    # Tests removeBucket api command.

    # get a random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('removeBucket')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      removeBucketWrapper(bucket_name, log_output)
      if !bucketExistsWrapper(bucket_name, log_output)
        log_output[:status] = 'PASS'
      else
        log_output[:error] = 'Bucket expected to be removed still exists'
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def putObjectTest(file)
    # Tests putObject api command by uploading a file

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('putObject')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      putObjectWrapper(bucket_name, file, log_output)
      if statObjectWrapper(bucket_name, File.basename(file), log_output)
        log_output[:status] = 'PASS'
      else
        log_output[:error] = "Status for the created object returned 'false'"
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def removeObjectTest(file)
    # Tests removeObject api command by uploading and removing a file

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('removeObject')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      putObjectWrapper(bucket_name, file, log_output)
      removeObjectWrapper(bucket_name, File.basename(file), log_output)
      if !statObjectWrapper(bucket_name, File.basename(file), log_output)
        log_output[:status] = 'PASS'
      else
        log_output[:error] = "Status for the removed object returned 'true'"
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def getObjectTest(file, destination)
    # Tests getObject api command

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('getObject')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      putObjectWrapper(bucket_name, file, log_output)
      getObjectWrapper(bucket_name, file, destination, log_output)
      if system("ls -l #{destination} > /dev/null")
        log_output[:status] = 'PASS'
      else
        log_output[:error] = "Downloaded object does not exist at #{destination}"
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def listObjectsTest(file_list)
    # Tests listObjects api command

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('listObjects')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      # Put all objects into the bucket
      file_list.each do |f|
        putObjectWrapper(bucket_name, f, log_output)
      end
      # Total number of files uploaded
      expected_no = file_list.length
      # Actual number is what api returns
      actual_no = listObjectsWrapper(bucket_name, log_output).length
      # Compare expected and actual values
      if expected_no == actual_no
        log_output[:status] = 'PASS'
      else
        log_output[:error] = 'Expected and actual number of listed files/objects do not match!'
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def copyObjectTest(data_dir, source_file_name, target_file_name = '')
    # Tests copyObject api command

    # get random bucket names
    source_bucket_name = random_bucket_name
    target_bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('copyObject')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      target_file_name = source_file_name if target_file_name.empty?
      makeBucketWrapper(source_bucket_name, log_output)
      makeBucketWrapper(target_bucket_name, log_output)
      putObjectWrapper(source_bucket_name,
                File.join(data_dir, source_file_name), log_output)
      copyObjectWrapper(source_bucket_name, target_bucket_name,
                 source_file_name, target_file_name, log_output)
      # Check if copy worked fine
      if statObjectWrapper(target_bucket_name, target_file_name, log_output)
        log_output[:status] = 'PASS'
      else
        log_output[:error] = 'Copied file could not be found in the expected location'
        log_output[:status] = 'FAIL'
      end
      cleanUp([source_bucket_name, target_bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def presignedGetObjectTest(data_dir, file_name)
    # Tests presignedGetObject api command

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('presignedGet')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      file = File.join(data_dir, file_name)
      # Get check sum value without the file name
      cksum_orig = `cksum #{file}`.split[0..1]
      putObjectWrapper(bucket_name, file, log_output)
      get_url = presignedGetWrapper(bucket_name, file_name, log_output)
      # Download the file using the URL
      # generated by presignedGet api command
      `wget -O /tmp/#{file_name} '#{get_url}' > /dev/null 2>&1`
      # Get check sum value for the downloaded file
      # Split to get rid of the file name
      cksum_new = `cksum /tmp/#{file_name}`.split[0..1]

      # Check if check sum values for the orig file
      # and the downloaded file match
      if cksum_orig == cksum_new
        log_output[:status] = 'PASS'
      else
        log_output[:error] = 'Check sum values do NOT match'
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def presignedPutObjectTest(data_dir, file_name)
    # Tests presignedPutObject api command

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('presignedPut')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      file = File.join(data_dir, file_name)

      # Get check sum value and
      # split to get rid of the file name
      cksum_orig = `cksum #{file}`.split[0..1]

      # Generate presigned Put URL and parse it
      uri = URI.parse(presignedPutWrapper(bucket_name, file_name, log_output))
      request = Net::HTTP::Put.new(uri.request_uri, 'x-amz-acl' => 'public-read')
      request.body = IO.read(File.join(data_dir, file_name))

      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true if ENV['ENABLE_HTTPS'] == '1'

      http.request(request)

      if statObjectWrapper(bucket_name, file_name, log_output)
        getObjectWrapper(bucket_name, file_name, '/tmp', log_output)
        cksum_new = `cksum /tmp/#{file_name}`.split[0..1]
        # Check if check sum values of the orig file
        # and the downloaded file match
        if cksum_orig == cksum_new
          log_output[:status] = 'PASS'
        else
          log_output[:error] = 'Check sum values do NOT match'
          log_output[:status] = 'FAIL'
        end
      else
        log_output[:error] = 'Expected to be created object does NOT exist'
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end

  def presignedPostObjectTest(data_dir, file_name,
                              expires_in_sec, max_byte_size)
    # Tests presignedPostObject api command

    # get random bucket name
    bucket_name = random_bucket_name
    # Initialize hash table, 'log_output'
    log_output = initialize_log_output('presignedPost')
    # Prepare arg/value hash table and set it in log_output
    arg_value_hash = {}
    log_output[:args].each { |x| arg_value_hash[:"#{x}"] = eval x.to_s }
    log_output[:args] = arg_value_hash

    begin
      start_time = Time.now
      makeBucketWrapper(bucket_name, log_output)
      file = File.join(data_dir, file_name)

      # Get check sum value and split it
      # into parts to get rid of the file name
      cksum_orig = `cksum #{file}`.split[0..1]
      # Create the presigned POST url
      post = presignedPostWrapper(bucket_name, file_name,
                                  expires_in_sec, max_byte_size, log_output)

      # Prepare multi parts array for POST command request
      file_part = Part.new name: 'file',
                           body: IO.read(File.join(data_dir, file_name)),
                           filename: file_name,
                           content_type: 'application/octet-stream'
      parts = [file_part]
      # Add POST fields into parts array
      post.fields.each do |field, value|
        parts.push(Part.new(field, value))
      end
      boundary = "---------------------------#{rand(10_000_000_000_000_000)}"
      body_parts = MultipartBody.new parts, boundary

      # Parse presigned Post URL
      uri = URI.parse(post.url)

      # Create the HTTP objects
      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true if ENV['ENABLE_HTTPS'] == '1'
      request = Net::HTTP::Post.new(uri.request_uri)
      request.body = body_parts.to_s
      request.content_type = "multipart/form-data; boundary=#{boundary}"
      # Send the request
      log_output[:error] = http.request(request)

      if statObjectWrapper(bucket_name, file_name, log_output)
        getObjectWrapper(bucket_name, file_name, '/tmp', log_output)
        cksum_new = `cksum /tmp/#{file_name}`.split[0..1]
        # Check if check sum values of the orig file
        # and the downloaded file match
        if cksum_orig == cksum_new
          log_output[:status] = 'PASS'
          # FIXME: HTTP No Content error, status code=204 is returned as error
          log_output[:error] = nil
        else
          log_output[:error] = 'Check sum values do NOT match'
          log_output[:status] = 'FAIL'
        end
      else
        log_output[:error] = 'Expected to be created object does NOT exist'
        log_output[:status] = 'FAIL'
      end
      cleanUp([bucket_name], log_output)
    rescue => log_output[:error]
      log_output[:status] = 'FAIL'
    end

    print_log(log_output, start_time)
  end
end

# MAIN CODE

# Create test Class instance and call the tests
aws = AwsSdkRubyTest.new
file_name1 = 'datafile-1-kB'
file_new_name = 'datafile-1-kB-copy'
file_name_list = ['datafile-1-kB', 'datafile-1-b', 'datafile-6-MB']
# Add data_dir in front of each file name in file_name_list
# The location where the bucket and file
# objects are going to be created.
data_dir = ENV['MINT_DATA_DIR'] ||= 'MINT_DATA_DIR is not set'
file_list = file_name_list.map { |f| File.join(data_dir, f) }
destination = '/tmp'

aws.listBucketsTest
aws.listObjectsTest(file_list)
aws.makeBucketTest
aws.bucketExistsNegativeTest
aws.removeBucketTest
aws.putObjectTest(File.join(data_dir, file_name1))
aws.removeObjectTest(File.join(data_dir, file_name1))
aws.getObjectTest(File.join(data_dir, file_name1), destination)
aws.copyObjectTest(data_dir, file_name1)
aws.copyObjectTest(data_dir, file_name1, file_new_name)
aws.presignedGetObjectTest(data_dir, file_name1)
aws.presignedPutObjectTest(data_dir, file_name1)
aws.presignedPostObjectTest(data_dir, file_name1, 60, 3*1024*1024)

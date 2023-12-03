#! /usr/bin/env bash

usage() {
	echo "
$0
    Usage: $0 [--alias=minioalias] minio_endpoint [web_identity_path]

This assumes that the 'web_identity.py' service is running at minio_endpoint/web_identity_path

Arguments:
    --alias=minioalias: If this flag is set the script will run the following:
    mc alias \${minioalias} \${minio_endpoint} \${access_key_id} \${secret_access_key}

    If the --alias= flag is not set then the output will be a set of variable assignments suitable for
    exporting into the environment: WI_ACCESS_KEY_ID, WI_SECRET_ACCESS_KEY, WI_EXPIRATION
    minio_endpoint: Should be the url of your minio deployment: e.g. https://minio.example.net
    web_identity_path: Path to the web-identity-service
        If it does not start with http then it will be added to minio_endpoint

    You could also source this script to get those variables, e.g.:
        source $0 minio_endpoint
"
}

# Probably should use jq or node but then everyone needs it
get_val() {
	local key=$1
	local json=$2
	grep -oP '(?<="'$key'": ")[^"]*' <<<"$json"
}

web_identity_login() {
	local alias_name=
	local endpoint_url=$1
	local web_identity_path=${2:-web-identity}
	local is_sourced=0
	local error=1
	if [[ ${BASH_SOURCE[0]} != "${0}" ]] || [[ ${ZSH_EVAL_CONTEXT##*:file} != "${ZSH_EVAL_CONTEXT-}" ]]; then
		is_sourced=1
		# never exit from sourced script!
		error=0
	else
		set -eu
	fi
	if [[ ${1:-} =~ ^--alias= ]]; then
		alias_name="${arg#--alias=}"
		shift
	fi
	if [[ ${1:-} == "--help" ]]; then
		usage
		return $error
	fi
	local web_identity_url="$endpoint_url/$web_identity_path"
	if [[ $web_identity_path =~ https?:// ]]; then
		web_identity_url="$web_identity_path"
	fi
	local curl_fail_flag=
	if curl --help | grep -- "--fail-with-body"; then
		curl_fail_flag="--fail-with-body"
	else
		curl_fail_flag="--fail"
	fi
	tmperr="$(mktemp "$dir/tmp-err.XXXXXXXX")"
	local trap_sig=RETURN
	if [ -n "$ZSH_VERSION" ]; then
		trap_sig=EXIT
	fi
	trap "rm $tmperr 2>/dev/null" $trap_sig
	auth_url="$web_identity_url/auth_url"
	# NOTE: %header only works on curl 7.83.0+
	local auth_url_response="$(curl $curl_fail_flag -s "$auth_url" -w "%{stderr}%{http_code} Location:%header{location}\n" -XPOST 2>"$tmperr")"
	if [ -z "$auth_url_response" ]; then
		echo "$(cat "$tmperr") error fetching auth url from $auth_url"
		return $error
	fi

	local nonce="$(get_val nonce "$auth_url_response")"
	local url="$(get_val url "$auth_url_response")"
	if [ -n "${WI_DEBUG:-}" ]; then
		echo nonce=$nonce url=$url >&2
	fi

	if [ -z "$url" ]; then
		echo "URL not found in response: $auth_url\n$(cat "$tmperr")\n$auth_url_response" >&2
		return $error
	fi
	echo "Opening auth url in browser: $url" >&2
	xdg-open "$url"
	auth_response_url="$web_identity_url/auth_response/$nonce"
	# %output only works past curl 8.3
	local auth_response="$(curl $curl_fail_flag -s "$auth_response_url" -w "%{stderr}%{http_code}\n" -XPOST 2>"$tmperr")"
	if [ -z "$auth_response" ]; then
		echo "$(cat "$tmperr") error fetching auth response from $auth_response_url" >&2
		return $error
	fi
	if [ -n "${WI_DEBUG:-}" ]; then
		echo "auth_response=$auth_response" >&2
	fi
	WI_ACCESS_KEY_ID="$(get_val AccessKeyId "$auth_response")"
	WI_SECRET_ACCESS_KEY="$(get_val SecretAccessKey "$auth_response")"
	WI_SESSION_TOKEN="$(get_val SessionToken "$auth_response")"
	WI_EXPIRATION="$(get_val Expiration "$auth_response")"

	if [ -n "$alias_name" ]; then
		echo "Creating MC alias '$alias_name', expires at $WI_EXPIRATION"
		mc alias set "$alias_name" "$endpoint_url" "$access_key_id" "$secret_access_key" >&2
	elif [ $is_sourced = 0 ]; then
		echo "
WI_ACCESS_KEY_ID=\"$WI_ACCESS_KEY_ID\"
WI_SECRET_ACCESS_KEY=\"$WI_SECRET_ACCESS_KEY\"
WI_SESSION_TOKEN=\"$WI_SESSION_TOKEN\"
WI_EXPIRATION=\"$WI_EXPIRATION\"
"
	else
		echo "Minio login successful. Expiration: $WI_EXPIRATION"
	fi
}

web_identity_login "$@"
result=$?
if [ $result != 0 ]; then
	exit $result
fi

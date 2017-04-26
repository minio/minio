# Minio Installation on macOS

## Fresh Install

Install Minio on macOS via brew.

```
brew install minio/stable/minio
minio server ~/Photos
```

## Upgrade 

Step 1: Uninstall minio if you installed it using `brew install minio`

```
brew uninstall minio 
```
Step 2: Fresh Install using new path

Once you remove minio completely from your system, proceed to do :

```
brew install minio/stable/minio
```

## Important Breaking Change  

#### Installation Path Changes for minio in brew

> `brew upgrade minio` and `brew install minio` commands will no longer install the latest minio binaries on macOS. Use `brew install minio/stable/minio` in all your brew paths.

Upstream bugs in golang 1.8 broke Minio brew installer. We will re-enable minio installation on macOS via `brew install minio` at a later date.

 

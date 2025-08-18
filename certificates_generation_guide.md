# 证书生成指南

## 1. 创建证书目录
```powershell
cd C:\ProteinFlow\AgentInteractionProtocol
mkdir credentials
cd credentials
```

## 2. 安装OpenSSL
下载地址: [https://slproweb.com/products/Win32OpenSSL.html](https://slproweb.com/products/Win32OpenSSL.html)

## 3. 证书生成脚本

### CA证书
1. **生成CA私钥 (ca.key)**  
   ```ps
   openssl genrsa -out ca.key 2048
   ```

2. **生成自签名CA根证书 (ca.crt)**  
   ```ps
   openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt -subj "/C=CN/ST=Beijing/L=Beijing/O=ProteinFlow Inc/CN=ProteinFlow Root CA"
   ```

### 服务器证书
1. **生成服务器私钥 (server.key)**  
   ```ps
   openssl genrsa -out server.key 2048
   ```

2. **创建证书签名请求 (server.csr)**  
   ```ps
   openssl req -new -key server.key -out server.csr -subj "/C=CN/ST=Beijing/L=Beijing/O=ProteinFlow Inc/CN=ProteinFlow Server"
   ```

3. **创建扩展配置文件 (server.ext)**  
   ```ps
   @"  
   authorityKeyIdentifier=keyid,issuer  
   basicConstraints=CA:FALSE  
   keyUsage = digitalSignature, keyEncipherment  
   subjectAltName = DNS:localhost, DNS:proteinflow.com, IP:127.0.0.1  
   extendedKeyUsage = serverAuth  
   "@ | Out-File server.ext
   ```

4. **签发服务器证书 (server.crt)**  
   ```ps
   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256 -extfile server.ext
   ```

### 客户端证书
1. **生成客户端私钥 (client.key)**  
   ```ps
   openssl genrsa -out client.key 2048
   ```

2. **创建证书签名请求 (client.csr)**  
   ```ps
   openssl req -new -key client.key -out client.csr -subj "/C=CN/ST=Beijing/L=Beijing/O=ProteinFlow Inc/CN=ProteinFlow Client"
   ```

3. **创建扩展配置文件 (client.ext)**  
   ```ps
   @"  
   authorityKeyIdentifier=keyid,issuer  
   basicConstraints=CA:FALSE  
   keyUsage = digitalSignature, keyEncipherment  
   extendedKeyUsage = clientAuth  
   "@ | Out-File client.ext
   ```

4. **签发客户端证书 (client.crt)**  
   ```ps
   openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAserial ca.srl -out client.crt -days 365 -sha256 -extfile client.ext
   ```

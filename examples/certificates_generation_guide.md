# Guide to Certificate Generation

## 1. Create the certificate directory
```powershell
cd path to your example
mkdir credentials
cd credentials
```

## 2. Install OpenSSL
Install address: [https://slproweb.com/products/Win32OpenSSL.html](https://slproweb.com/products/Win32OpenSSL.html)

## 3. Script for generating certificates

### CA certificate
1. **Generate CA private key (ca.key)**  
   ```ps
   openssl genrsa -out ca.key 2048
   ```

2. **Generate self_signed CA root certificate (ca.crt)**  
   ```ps
   openssl req -x509 -new -nodes -key ca.key -sha256 -days 3650 -out ca.crt -subj "Your certificate subject"
   ```

### Server certificate
1. **Generate server private key (server.key)**  
   ```ps
   openssl genrsa -out server.key 2048
   ```

2. **Create a certificate signing request (server.csr)**  
   ```ps
   openssl req -new -key server.key -out server.csr -subj "Your certificate subject"
   ```

3. **Create an extension configuration file (server.ext)**  
   ```ps
   @"  
   authorityKeyIdentifier=keyid,issuer  
   basicConstraints=CA:FALSE  
   keyUsage = digitalSignature, keyEncipherment  
   subjectAltName = DNS:localhost, DNS:example.com, IP:127.0.0.1  
   extendedKeyUsage = serverAuth  
   "@ | Out-File server.ext
   ```

4. **Issue server certificate (server.crt)**  
   ```ps
   openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt -days 365 -sha256 -extfile server.ext
   ```

### client certificate
1. **Generate the client private key (client.key)**  
   ```ps
   openssl genrsa -out client.key 2048
   ```

2. **Create a certificate signing request (client.csr)**  
   ```ps
   openssl req -new -key client.key -out client.csr -subj "Your certificate subject"
   ```

3. **Create an extension configuration file (client.ext)**  
   ```ps
   @"  
   authorityKeyIdentifier=keyid,issuer  
   basicConstraints=CA:FALSE  
   keyUsage = digitalSignature, keyEncipherment  
   extendedKeyUsage = clientAuth  
   "@ | Out-File client.ext
   ```

4. **Issue client certificate (client.crt)**  
   ```ps
   openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAserial ca.srl -out client.crt -days 365 -sha256 -extfile client.ext
   ```

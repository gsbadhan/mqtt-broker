```
certs/server/
├── server-ca.key       ← CA PRIVATE KEY
├── server-ca.crt       ← CA certificate
├── server.key          ← MQTT SERVER PRIVATE KEY
├── server.csr          ← temporary CSR
├── server.crt          ← MQTT SERVER CERTIFICATE
├── server-ext.cnf      ← certificate extensions
└── server-ca.srl       ← CA serial file


             Server CA
          server-ca.crt
                |
                | signs
                ↓
           server.crt
                |
                | paired with
                ↓
           server.key
           
certs/server/
│
├── server-ca.crt      ← CA certificate
├── server-ca.key      ← CA private key
│
├── server.crt         ← MQTT server certificate
└── server.key         ← MQTT server private key
           
```


## Step 1 — Create directories
```
mkdir -p certs/server
cd certs/server
```

## Step 2 — Create Server CA private key
```
openssl genrsa -out server-ca.key 4096
```

## Step 3 — Create Server CA certificate
```
openssl req -x509 \
-new \
-nodes \
-key server-ca.key \
-sha256 \
-days 3650 \
-out server-ca.crt \
-subj "/C=IN/ST=Delhi/L=Delhi/O=MyMQTT/OU=PKI/CN=DemoMQTT Server CA"
```
## Step 4 — Create the MQTT server private key
```
openssl genrsa -out server.key 2048
```

## Step 5 — Create a server CSR
```
openssl req -new \
-key server.key \
-out server.csr \
-subj "/C=IN/ST=Delhi/L=Delhi/O=MyMQTT/OU=MQTT/CN=demomqtt.company.com"
```

## Step 6 — Create a certificate extension
```
cat > server-ext.cnf <<'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=@alt_names

[alt_names]
DNS.1=localhost
DNS.2=demomqtt.company.com
IP.1=127.0.0.1
EOF
```

## Step 6 — Sign the certificate
```
openssl x509 -req \
-in server.csr \
-CA server-ca.crt \
-CAkey server-ca.key \
-CAcreateserial \
-out server.crt \
-days 825 \
-sha256 \
-extfile server-ext.cnf
```

## Step — verify certs
```
openssl x509 -in server.crt -text -noout

openssl verify -CAfile server-ca.crt server.crt

openssl x509 -in server.crt -text -noout | grep -A2 "Subject Alternative Name"
```
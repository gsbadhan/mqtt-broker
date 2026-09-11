```
device-ca.key
      |
      | signs
      ↓
device-ca.crt

certs/device/
├── device-ca.key
└── device-ca.crt


                    Device CA
                 device-ca.crt
                      |
                      | will sign
                      ↓
              +---------------+
              |               |
       device-001.crt   device-002.crt 


                 Device CA
              device-ca.crt
              device-ca.key
                   |
                   | signs
                   ↓
          ┌──────────────────┐
          │   device-001     │
          │                  │
          │ device-001.crt   │
          │ device-001.key   │
          └──────────────────┘

```


## Step 1 — Create directories
```
mkdir -p certs/device
cd certs/device
```

## Step 2 — Create Device CA private key
```
openssl genrsa -out device-ca.key 4096
```
## Step 3 — Create Device CA certificate
```
openssl req -x509 \
-new \
-nodes \
-key device-ca.key \
-sha256 \
-days 3650 \
-out device-ca.crt \
-subj "/C=IN/ST=Delhi/L=Delhi/O=MyMQTT/OU=PKI/CN=DemoMQTT Device CA"
```

## Step 4 — verify certs
```
openssl x509 -in device-ca.crt -text -noout

openssl x509 -in device-ca.crt -text -noout | grep -A2 "Basic Constraints"

```

## Step 5 — Generate private key for any device like device-001
```
openssl genrsa -out device-001.key 2048
```

## Step 4 — Create the device CSR for device-001
```
openssl req -new \
-key device-001.key \
-out device-001.csr \
-subj "/C=IN/ST=Delhi/L=Delhi/O=MyMQTT/OU=Devices/CN=device-001"

## Step 4 — Create the device certificate extensions for device-001
cat > device-001-ext.cnf <<'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth
subjectAltName=@alt_names

[alt_names]
URI.1=urn:device:device-001
EOF
```

## Step 8 — Sign the device certificate for device-001
```
openssl x509 -req \
-in device-001.csr \
-CA device-ca.crt \
-CAkey device-ca.key \
-CAcreateserial \
-out device-001.crt \
-days 825 \
-sha256 \
-extfile device-001-ext.cnf
```

## Step 9 — Verify the certificate
```
openssl verify \
-CAfile device-ca.crt \
device-001.crt

openssl x509 \
-in device-001.crt \
-text \
-noout
```

# Key Pair Authentication Setup

This guide shows you how to set up key pair authentication for the Snowflake MCP Server, enabling it to work in containerized environments (Docker, Kubernetes, etc.) without requiring browser-based SSO.

## Why Key Pair Authentication?

- ✅ **Works in containers** - No browser required
- ✅ **Automated deployments** - Perfect for CI/CD, serverless, etc.
- ✅ **Enhanced security** - Uses RSA public/private key pairs
- ✅ **Best practice** - Recommended for service accounts and applications

## Prerequisites

- Access to your Snowflake account with ACCOUNTADMIN role (or permission to alter users)
- OpenSSL or similar tool to generate keys
- Terminal/command line access

## Step 1: Generate RSA Key Pair

Generate an RSA private key (2048-bit minimum, 4096-bit recommended):

```bash
# Generate encrypted private key (recommended for production)
openssl genrsa -aes256 -out snowflake_key.pem 4096

# Or generate unencrypted private key (easier for development)
openssl genrsa -out snowflake_key.pem 4096
```

**Important:** Keep your private key secure! Never commit it to version control.

## Step 2: Generate Public Key

Extract the public key from the private key:

```bash
openssl rsa -in snowflake_key.pem -pubout -out snowflake_key.pub
```

## Step 3: Format Public Key for Snowflake

Snowflake requires the public key in a specific format (without header/footer and line breaks):

```bash
# Remove header, footer, and newlines
grep -v "BEGIN PUBLIC" snowflake_key.pub | \
grep -v "END PUBLIC" | \
tr -d '\n' > snowflake_key_formatted.txt

# Display the formatted key
cat snowflake_key_formatted.txt
```

## Step 4: Assign Public Key to Snowflake User

In Snowflake (via web UI or SnowSQL), run:

```sql
-- Replace YOUR_USERNAME with your actual username
-- Replace <formatted_public_key> with the key from snowflake_key_formatted.txt

USE ROLE ACCOUNTADMIN;

ALTER USER YOUR_USERNAME SET RSA_PUBLIC_KEY='<formatted_public_key>';

-- Verify the key was set
DESC USER YOUR_USERNAME;
```

**Example:**
```sql
ALTER USER NCEJDA@G2.COM SET RSA_PUBLIC_KEY='MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEA...rest_of_key...';
```

## Step 5: Configure MCP Server

### Option A: Environment Variables

```bash
export SNOWFLAKE_ACCOUNT="your-account-id"
export SNOWFLAKE_USERNAME="your-email@company.com"
export SNOWFLAKE_WAREHOUSE="YOUR_WAREHOUSE"
export SNOWFLAKE_AUTHENTICATOR="snowflake_jwt"
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/snowflake_key.pem"

# Optional: If your private key is encrypted
export SNOWFLAKE_PRIVATE_KEY_PASSPHRASE="your-passphrase"
```

### Option B: MCP Client Configuration

**For Docker:**
```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "docker",
      "args": ["run", "-i", "--rm",
               "-v", "/path/to/keys:/keys:ro",
               "-e", "SNOWFLAKE_ACCOUNT=your-account",
               "-e", "SNOWFLAKE_USERNAME=your-email@company.com",
               "-e", "SNOWFLAKE_WAREHOUSE=YOUR_WAREHOUSE",
               "-e", "SNOWFLAKE_AUTHENTICATOR=snowflake_jwt",
               "-e", "SNOWFLAKE_PRIVATE_KEY_PATH=/keys/snowflake_key.pem",
               "snowflake-mcp-server"]
    }
  }
}
```

**For npx:**
```json
{
  "mcpServers": {
    "snowflake-readonly": {
      "command": "npx",
      "args": ["snowflake-mcp-server"],
      "env": {
        "SNOWFLAKE_ACCOUNT": "your-account",
        "SNOWFLAKE_USERNAME": "your-email@company.com",
        "SNOWFLAKE_WAREHOUSE": "YOUR_WAREHOUSE",
        "SNOWFLAKE_AUTHENTICATOR": "snowflake_jwt",
        "SNOWFLAKE_PRIVATE_KEY_PATH": "/Users/you/.snowflake/snowflake_key.pem"
      }
    }
  }
}
```

## Step 6: Test the Connection

Test your key pair authentication:

```bash
# Set environment variables
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USERNAME="your-email@company.com"
export SNOWFLAKE_WAREHOUSE="YOUR_WAREHOUSE"
export SNOWFLAKE_AUTHENTICATOR="snowflake_jwt"
export SNOWFLAKE_PRIVATE_KEY_PATH="/path/to/snowflake_key.pem"

# Run the server
npx snowflake-mcp-server
# or
uvx --from . snowflake-mcp
# or
docker run -it --rm \
  -v /path/to/keys:/keys:ro \
  -e SNOWFLAKE_ACCOUNT="your-account" \
  -e SNOWFLAKE_USERNAME="your-email@company.com" \
  -e SNOWFLAKE_WAREHOUSE="YOUR_WAREHOUSE" \
  -e SNOWFLAKE_AUTHENTICATOR="snowflake_jwt" \
  -e SNOWFLAKE_PRIVATE_KEY_PATH="/keys/snowflake_key.pem" \
  snowflake-mcp-server
```

## Security Best Practices

1. **Never commit private keys** - Add `*.pem` to `.gitignore`
2. **Use encrypted keys in production** - Always use passphrase-protected keys
3. **Rotate keys regularly** - Update keys periodically
4. **Limit key access** - Use file permissions (chmod 600)
5. **Use separate keys per environment** - Different keys for dev/staging/prod
6. **Store keys securely** - Use secret managers (AWS Secrets Manager, HashiCorp Vault, etc.)

## Recommended File Permissions

```bash
# Restrict access to private key
chmod 600 snowflake_key.pem

# Public key can be more permissive
chmod 644 snowflake_key.pub
```

## Troubleshooting

### "Failed to load private key"
- Check that `SNOWFLAKE_PRIVATE_KEY_PATH` points to the correct file
- Verify file permissions (should be readable by the user running the server)
- If encrypted, ensure `SNOWFLAKE_PRIVATE_KEY_PASSPHRASE` is set

### "Authentication failed"
- Verify the public key was correctly assigned in Snowflake (`DESC USER username`)
- Ensure the `SNOWFLAKE_USERNAME` matches exactly (case-sensitive)
- Check that the private key matches the public key uploaded to Snowflake

### "Private key format error"
- Ensure you're using PEM format (not DER or other formats)
- Verify the key was generated with RSA (not ECDSA or other algorithms)
- Try generating a new key pair if the existing one is corrupted

## Docker-Specific Configuration

When using Docker, you need to mount your private key into the container:

```bash
# Build the image
docker build -t snowflake-mcp-server .

# Run with key pair auth
docker run -it --rm \
  -v ~/.snowflake/snowflake_key.pem:/app/snowflake_key.pem:ro \
  -e SNOWFLAKE_ACCOUNT="your-account" \
  -e SNOWFLAKE_USERNAME="your-email@company.com" \
  -e SNOWFLAKE_WAREHOUSE="YOUR_WAREHOUSE" \
  -e SNOWFLAKE_AUTHENTICATOR="snowflake_jwt" \
  -e SNOWFLAKE_PRIVATE_KEY_PATH="/app/snowflake_key.pem" \
  snowflake-mcp-server
```

## Kubernetes Secret Example

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: snowflake-key
type: Opaque
data:
  snowflake_key.pem: <base64-encoded-private-key>
---
apiVersion: v1
kind: Pod
metadata:
  name: snowflake-mcp
spec:
  containers:
  - name: mcp-server
    image: snowflake-mcp-server:latest
    env:
    - name: SNOWFLAKE_ACCOUNT
      value: "your-account"
    - name: SNOWFLAKE_USERNAME
      value: "your-email@company.com"
    - name: SNOWFLAKE_WAREHOUSE
      value: "YOUR_WAREHOUSE"
    - name: SNOWFLAKE_AUTHENTICATOR
      value: "snowflake_jwt"
    - name: SNOWFLAKE_PRIVATE_KEY_PATH
      value: "/keys/snowflake_key.pem"
    volumeMounts:
    - name: key-volume
      mountPath: /keys
      readOnly: true
  volumes:
  - name: key-volume
    secret:
      secretName: snowflake-key
      defaultMode: 0400
```

## Additional Resources

- [Snowflake Key Pair Authentication Documentation](https://docs.snowflake.com/en/user-guide/key-pair-auth)
- [OpenSSL Documentation](https://www.openssl.org/docs/)
- [Docker Secrets](https://docs.docker.com/engine/swarm/secrets/)

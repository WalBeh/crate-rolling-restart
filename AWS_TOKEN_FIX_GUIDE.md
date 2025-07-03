# AWS Token Fix Guide

## Overview

This guide helps resolve AWS token expiration issues when working with EKS clusters through the CrateDB Temporal worker.

## Common Error Messages

```
An error occurred (ExpiredToken) when calling the AssumeRole operation: The security token included in the request is expired
```

```
ERROR:temporalio.activity:Error discovering clusters: (401)
Reason: Unauthorized
```

```
Kubernetes authentication failed. Check your credentials and ensure AWS token is not expired.
```

## Root Cause

The issue occurs when:
1. Your AWS session token has expired
2. The kubeconfig is configured to use AWS IAM authentication for EKS
3. The worker tries to access Kubernetes resources but can't authenticate

## Solutions

### Option 1: Refresh AWS Credentials (Recommended)

#### Using AWS SSO
```bash
# Re-login to AWS SSO
aws sso login --profile your-profile-name

# Or if using default profile
aws sso login
```

#### Using AWS CLI with credentials
```bash
# Re-configure your credentials
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_SESSION_TOKEN=your-session-token  # if using temporary credentials
```

#### Using AWS STS (for temporary credentials)
```bash
# Get new temporary credentials
aws sts get-session-token --duration-seconds 3600

# Apply the returned credentials to your environment
export AWS_ACCESS_KEY_ID=new-access-key
export AWS_SECRET_ACCESS_KEY=new-secret-key
export AWS_SESSION_TOKEN=new-session-token
```

### Option 2: Update Kubeconfig

#### Refresh EKS kubeconfig
```bash
# Update kubeconfig for your EKS cluster
aws eks update-kubeconfig --region us-east-1 --name your-cluster-name

# Or if using a specific profile
aws eks update-kubeconfig --region us-east-1 --name your-cluster-name --profile your-profile
```

#### Verify kubeconfig
```bash
# Test Kubernetes access
kubectl get nodes

# Check current context
kubectl config current-context

# List available contexts
kubectl config get-contexts
```

### Option 3: Use Different Authentication Method

#### Service Account Token (for CI/CD)
```bash
# Create a service account with appropriate permissions
kubectl create serviceaccount cratedb-operator
kubectl create clusterrolebinding cratedb-operator --clusterrole=cluster-admin --serviceaccount=default:cratedb-operator

# Get the token
kubectl get secret $(kubectl get serviceaccount cratedb-operator -o jsonpath='{.secrets[0].name}') -o jsonpath='{.data.token}' | base64 --decode
```

#### Direct Certificate Authentication
```bash
# Extract client certificate and key from kubeconfig
kubectl config view --raw -o jsonpath='{.users[0].user.client-certificate-data}' | base64 --decode > client.crt
kubectl config view --raw -o jsonpath='{.users[0].user.client-key-data}' | base64 --decode > client.key
```

## Verification Steps

### 1. Test AWS Authentication
```bash
# Check AWS identity
aws sts get-caller-identity

# Should return your user/role information without errors
```

### 2. Test Kubernetes Access
```bash
# Test basic access
kubectl get namespaces

# Test CRD access (for CrateDB operator)
kubectl get cratedbs --all-namespaces
```

### 3. Test Worker Connection
```bash
# Run a quick discovery test
python -c "
import asyncio
import sys
sys.path.insert(0, '.')

from rr.activities import CrateDBActivities
from rr.models import ClusterDiscoveryInput

async def test():
    activities = CrateDBActivities()
    result = await activities.discover_clusters(ClusterDiscoveryInput())
    print(f'Found {result.total_found} clusters')
    if result.errors:
        print(f'Errors: {result.errors}')

asyncio.run(test())
"
```

## Prevention

### 1. Use Long-Lived Credentials
- Configure AWS CLI with IAM user credentials instead of temporary tokens
- Use AWS SSO with automatic renewal
- Set up credential rotation scripts

### 2. Monitor Token Expiration
```bash
# Check when your credentials expire
aws sts get-caller-identity --query 'Arn' --output text
aws configure list
```

### 3. Automate Credential Refresh
Create a script to automatically refresh credentials:

```bash
#!/bin/bash
# refresh-aws-creds.sh

# Check if credentials are expired
if ! aws sts get-caller-identity >/dev/null 2>&1; then
    echo "AWS credentials expired, refreshing..."
    
    # Refresh based on your authentication method
    aws sso login --profile your-profile
    
    # Update kubeconfig
    aws eks update-kubeconfig --region us-east-1 --name your-cluster-name
    
    echo "Credentials refreshed successfully"
else
    echo "AWS credentials are still valid"
fi
```

## Troubleshooting

### Check Current Configuration
```bash
# Check AWS configuration
aws configure list
cat ~/.aws/config
cat ~/.aws/credentials

# Check Kubernetes configuration
kubectl config view
kubectl config current-context
```

### Debug Authentication Issues
```bash
# Enable AWS CLI debug logging
export AWS_CLI_LOG_LEVEL=debug
aws sts get-caller-identity

# Test kubectl with verbose logging
kubectl get nodes -v=8
```

### Common Issues and Solutions

#### Issue: "No credentials found"
**Solution**: Run `aws configure` or set environment variables

#### Issue: "Token refresh failed"
**Solution**: Re-login to AWS SSO or refresh your session

#### Issue: "Context not found"
**Solution**: Update kubeconfig with `aws eks update-kubeconfig`

#### Issue: "Permission denied"
**Solution**: Ensure your AWS user/role has EKS access permissions

## Best Practices

1. **Use AWS SSO** for better credential management
2. **Set up credential rotation** to avoid manual intervention
3. **Monitor credential expiration** with automated checks
4. **Use IAM roles** with appropriate permissions
5. **Keep kubeconfig updated** when switching contexts
6. **Test connectivity** before running long workflows

## Emergency Recovery

If you're completely locked out:

1. **Reset AWS credentials**:
   ```bash
   aws configure
   # Enter new access key and secret
   ```

2. **Recreate kubeconfig**:
   ```bash
   rm ~/.kube/config
   aws eks update-kubeconfig --region us-east-1 --name your-cluster-name
   ```

3. **Verify access**:
   ```bash
   kubectl get nodes
   ```

4. **Restart the worker**:
   ```bash
   # Kill existing worker
   pkill -f "python -m rr.worker"
   
   # Start fresh worker
   uv run python -m rr.worker
   ```

## Contact and Support

If you continue to experience issues:
1. Check the Temporal worker logs for specific error messages
2. Verify your AWS IAM permissions include EKS access
3. Ensure your EKS cluster is accessible from your network
4. Contact your AWS administrator for credential management help

Remember: The CrateDB operator now handles authentication errors gracefully and will provide clear error messages to help diagnose credential issues.
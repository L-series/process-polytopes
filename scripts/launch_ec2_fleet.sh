#!/usr/bin/env bash
# launch_ec2_fleet.sh — Launch EC2 spot fleet for polytope classification
#
# Launches 3× c7a.24xlarge spot instances to process files 1000-3999,
# while the Ryzen handles files 500-999 locally.
#
# Ryzen (0-499 done, 500-999 local) + 3× EC2 (1000-3999)
# Budget:  $200 (spot instances + S3 + EBS)
# Quota:   300 vCPU → 3×96 = 288 vCPU used
#
# Prerequisites:
#   - AWS CLI v2 configured (aws configure / CloudShell)
#   - Your HuggingFace token
#   - Your GitHub repo URL (or make the repo public)
#
# Usage:
#   export HF_TOKEN="hf_..."
#   ./scripts/launch_ec2_fleet.sh
#
# Or override defaults:
#   REGION=us-east-2 KEY_NAME=mykey ./scripts/launch_ec2_fleet.sh
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

# ── Configuration ─────────────────────────────────────────────────────────

REGION="${REGION:-us-east-1}"
KEY_NAME="${KEY_NAME:-polytope-key}"
BUCKET_NAME="${BUCKET_NAME:-polytope-checkpoints-$(date +%s | tail -c 8)}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c7a.24xlarge}"        # 96 vCPU, 192 GB RAM
FALLBACK_TYPE="${FALLBACK_TYPE:-c6a.24xlarge}"        # 96 vCPU, 192 GB RAM
AMI_ID="${AMI_ID:-}"                                   # auto-detect Ubuntu 24.04
REPO_URL="${REPO_URL:-https://github.com/ahatziiliou/process-polytopes.git}"
BATCH_SIZE="${BATCH_SIZE:-50}"
SPOT_MAX_PRICE="${SPOT_MAX_PRICE:-1.80}"              # $/hr max bid per instance
EBS_SIZE="${EBS_SIZE:-120}"                            # GB per instance
# Set USE_SPOT=false to launch on-demand (higher cost but works before spot quota approved)
USE_SPOT="${USE_SPOT:-true}"
# GitHub personal access token — only needed if the repo is private
GITHUB_TOKEN="${GITHUB_TOKEN:-}"

# File ranges — Ryzen does 500-999 locally, EC2 does 1000-3999
FILE_START=1000
FILE_END=3999
TOTAL_FILES=$(( FILE_END - FILE_START + 1 ))  # 3000
NUM_INSTANCES=3
FILES_PER_INSTANCE=$(( TOTAL_FILES / NUM_INSTANCES ))  # 1000

# HuggingFace token
if [[ -z "${HF_TOKEN:-}" ]]; then
    echo "Error: Set HF_TOKEN environment variable"
    echo "  export HF_TOKEN='hf_...'"
    exit 1
fi

echo "═══════════════════════════════════════════════════════════════"
echo " EC2 Spot Fleet — Polytope Classifier"
echo "═══════════════════════════════════════════════════════════════"
echo " Region:          $REGION"
echo " Instance type:   $INSTANCE_TYPE (fallback: $FALLBACK_TYPE)"
echo " EC2 instances:   $NUM_INSTANCES"
echo " vCPUs (EC2):     $(( NUM_INSTANCES * 96 ))"
echo " EC2 files:       $FILE_START .. $FILE_END ($TOTAL_FILES files)"
echo " Files/instance:  $FILES_PER_INSTANCE"
echo " Ryzen (local):   files 500–999 (500 files)"
echo " S3 bucket:       $BUCKET_NAME"
echo " Max spot price:  \$$SPOT_MAX_PRICE/hr"
echo "═══════════════════════════════════════════════════════════════"
echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 1: Create S3 bucket
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 1: Create S3 bucket ==="

if aws s3api head-bucket --bucket "$BUCKET_NAME" --region "$REGION" 2>/dev/null; then
    echo "  Bucket $BUCKET_NAME already exists"
else
    if [[ "$REGION" == "us-east-1" ]]; then
        aws s3api create-bucket \
            --bucket "$BUCKET_NAME" \
            --region "$REGION"
    else
        aws s3api create-bucket \
            --bucket "$BUCKET_NAME" \
            --region "$REGION" \
            --create-bucket-configuration LocationConstraint="$REGION"
    fi
    echo "  ✓ Created bucket: $BUCKET_NAME"
fi

# Enable intelligent tiering to save on storage costs
aws s3api put-bucket-lifecycle-configuration \
    --bucket "$BUCKET_NAME" \
    --lifecycle-configuration '{
        "Rules": [{
            "ID": "auto-cleanup",
            "Status": "Enabled",
            "Filter": {"Prefix": ""},
            "Transitions": [{
                "Days": 30,
                "StorageClass": "GLACIER"
            }],
            "Expiration": {"Days": 90}
        }]
    }' 2>/dev/null || true

echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 2: Create IAM role for EC2 → S3 access
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 2: Create IAM role ==="

ROLE_NAME="polytope-ec2-role"
PROFILE_NAME="polytope-ec2-profile"

# Trust policy: allow EC2 to assume this role
TRUST_POLICY='{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Principal": {"Service": "ec2.amazonaws.com"},
        "Action": "sts:AssumeRole"
    }]
}'

if aws iam get-role --role-name "$ROLE_NAME" 2>/dev/null; then
    echo "  Role $ROLE_NAME already exists"
else
    aws iam create-role \
        --role-name "$ROLE_NAME" \
        --assume-role-policy-document "$TRUST_POLICY" \
        --description "Polytope classifier EC2 role - S3 checkpoint access"
    echo "  ✓ Created role: $ROLE_NAME"
fi

# S3 access policy — scoped to our bucket only
S3_POLICY='{
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Allow",
        "Action": [
            "s3:PutObject",
            "s3:GetObject",
            "s3:ListBucket",
            "s3:DeleteObject"
        ],
        "Resource": [
            "arn:aws:s3:::'"$BUCKET_NAME"'",
            "arn:aws:s3:::'"$BUCKET_NAME"'/*"
        ]
    }]
}'

aws iam put-role-policy \
    --role-name "$ROLE_NAME" \
    --policy-name "polytope-s3-access" \
    --policy-document "$S3_POLICY"

# Create instance profile
if aws iam get-instance-profile --instance-profile-name "$PROFILE_NAME" 2>/dev/null; then
    echo "  Instance profile $PROFILE_NAME already exists"
else
    aws iam create-instance-profile \
        --instance-profile-name "$PROFILE_NAME"
    aws iam add-role-to-instance-profile \
        --instance-profile-name "$PROFILE_NAME" \
        --role-name "$ROLE_NAME"
    echo "  ✓ Created instance profile: $PROFILE_NAME"
    echo "  Waiting 10s for IAM propagation..."
    sleep 10
fi

echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 3: Create key pair (if needed)
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 3: Create key pair ==="

if aws ec2 describe-key-pairs --key-names "$KEY_NAME" --region "$REGION" 2>/dev/null; then
    echo "  Key pair $KEY_NAME already exists"
else
    aws ec2 create-key-pair \
        --key-name "$KEY_NAME" \
        --region "$REGION" \
        --query 'KeyMaterial' \
        --output text > "${KEY_NAME}.pem"
    chmod 400 "${KEY_NAME}.pem"
    echo "  ✓ Created key pair: ${KEY_NAME}.pem"
    echo "  ⚠ SAVE THIS FILE — you cannot download it again!"
fi

echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 4: Create security group
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 4: Create security group ==="

SG_NAME="polytope-classifier-sg"
VPC_ID=$(aws ec2 describe-vpcs --region "$REGION" \
    --filters "Name=is-default,Values=true" \
    --query 'Vpcs[0].VpcId' --output text)

SG_ID=$(aws ec2 describe-security-groups --region "$REGION" \
    --filters "Name=group-name,Values=$SG_NAME" "Name=vpc-id,Values=$VPC_ID" \
    --query 'SecurityGroups[0].GroupId' --output text 2>/dev/null || echo "None")

if [[ "$SG_ID" != "None" && -n "$SG_ID" ]]; then
    echo "  Security group $SG_NAME already exists: $SG_ID"
else
    SG_ID=$(aws ec2 create-security-group \
        --group-name "$SG_NAME" \
        --description "Polytope classifier — SSH only" \
        --vpc-id "$VPC_ID" \
        --region "$REGION" \
        --query 'GroupId' --output text)

    # Allow SSH from anywhere (you can restrict to your IP)
    aws ec2 authorize-security-group-ingress \
        --group-id "$SG_ID" \
        --region "$REGION" \
        --protocol tcp --port 22 --cidr 0.0.0.0/0

    echo "  ✓ Created security group: $SG_ID"
fi

echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 5: Find Ubuntu 24.04 AMI
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 5: Find AMI ==="

if [[ -z "$AMI_ID" ]]; then
    AMI_ID=$(aws ec2 describe-images --region "$REGION" \
        --owners 099720109477 \
        --filters \
            "Name=name,Values=ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*" \
            "Name=state,Values=available" \
        --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
        --output text)
fi

echo "  AMI: $AMI_ID (Ubuntu 24.04)"
echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 6: Check spot pricing
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 6: Current spot prices ==="

for itype in "$INSTANCE_TYPE" "$FALLBACK_TYPE"; do
    PRICE=$(aws ec2 describe-spot-price-history --region "$REGION" \
        --instance-types "$itype" \
        --product-descriptions "Linux/UNIX" \
        --start-time "$(date -u +%Y-%m-%dT%H:%M:%S)" \
        --query 'SpotPriceHistory[0].SpotPrice' --output text 2>/dev/null || echo "N/A")
    echo "  $itype: \$$PRICE/hr"
done

echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 7: Launch spot instances
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 7: Launching $NUM_INSTANCES spot instances ==="

INSTANCE_IDS=()

for i in $(seq 0 $(( NUM_INSTANCES - 1 ))); do
    # Calculate file range for this instance
    START=$(( FILE_START + i * FILES_PER_INSTANCE ))
    END=$(( START + FILES_PER_INSTANCE - 1 ))
    # Last instance takes any remainder
    if (( i == NUM_INSTANCES - 1 )); then
        END=$FILE_END
    fi

    RUNNER_TAG="runner-${i}-files-${START}-${END}"

    # User-data script — runs on first boot as root
    USERDATA=$(cat <<USERDATA_EOF
#!/bin/bash
exec > /var/log/polytope-setup.log 2>&1
set -euxo pipefail

echo "=== Polytope Classifier Setup — Runner $i ==="
echo "Files: $START .. $END"
echo "Bucket: s3://$BUCKET_NAME/checkpoints"

# Install base packages
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq \
    build-essential cmake ninja-build \
    python3-pip python3-venv \
    pkg-config git curl \
    lsb-release wget gnupg ca-certificates unzip

# Install AWS CLI v2 (not in Ubuntu apt repos)
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp/awscliv2
/tmp/awscliv2/aws/install
rm -rf /tmp/awscliv2 /tmp/awscliv2.zip

# Add Apache Arrow apt repository for Ubuntu 24.04 (noble)
# Key and source list per https://arrow.apache.org/install/
wget -q -O /usr/share/keyrings/apache-arrow-apt-keyring.asc \
    https://downloads.apache.org/arrow/KEYS
echo "deb [signed-by=/usr/share/keyrings/apache-arrow-apt-keyring.asc] https://apache.jfrog.io/artifactory/arrow/ubuntu noble main" \
    > /etc/apt/sources.list.d/apache-arrow.list
apt-get update -qq
apt-get install -y -qq libarrow-dev libparquet-dev libarrow-dataset-dev

# Python deps
python3 -m pip install --break-system-packages --quiet \
    huggingface_hub hf_transfer python-dotenv 2>/dev/null || \
python3 -m pip install --quiet \
    huggingface_hub hf_transfer python-dotenv

# Clone and build
WORK_DIR="/home/ubuntu/process-polytopes"
if [[ -d "\$WORK_DIR" ]]; then
    (cd "\$WORK_DIR" && git pull --ff-only)
else
    # GIT_TERMINAL_PROMPT=0 prevents git from trying to open /dev/tty
    # (which doesn't exist in user-data) even for public repos
    GIT_TERMINAL_PROMPT=0 git clone "$REPO_URL" "\$WORK_DIR"
fi
chown -R ubuntu:ubuntu "\$WORK_DIR"

(cd "\$WORK_DIR/src/classify" && sudo -u ubuntu ./build.sh build)

# Write .env
echo "HF_TOKEN=$HF_TOKEN" > "\$WORK_DIR/.env"
chown ubuntu:ubuntu "\$WORK_DIR/.env"

# CPU tuning
if [[ -d /sys/devices/system/cpu/cpu0/cpufreq ]]; then
    echo performance | tee /sys/devices/system/cpu/cpu*/cpufreq/scaling_governor 2>/dev/null || true
fi

# Launch the runner as ubuntu user
OUTPUT_DIR="/tmp/polytope-output"
mkdir -p "\$OUTPUT_DIR"
chown ubuntu:ubuntu "\$OUTPUT_DIR"

sudo -u ubuntu bash -c '
    export RUNNER_START=$START
    export RUNNER_END=$END
    export STORAGE_BACKEND=s3
    export CHECKPOINT_BUCKET="s3://$BUCKET_NAME/checkpoints"
    export HF_TOKEN="$HF_TOKEN"
    export BATCH_SIZE=$BATCH_SIZE
    export OUTPUT_DIR="/tmp/polytope-output"
    export THREADS=0
    cd /home/ubuntu/process-polytopes
    nohup bash scripts/run_ec2.sh > "\$OUTPUT_DIR/run.log" 2>&1 &
    echo \$! > "\$OUTPUT_DIR/runner.pid"
'

echo "=== Runner $i launched ==="
USERDATA_EOF
)

    echo ""
    echo "  Runner $i: files $START–$END"

    # Launch spot or on-demand instance
    MARKET_OPTIONS=""
    if [[ "$USE_SPOT" == "true" ]]; then
        MARKET_OPTIONS='--instance-market-options {"MarketType":"spot","SpotOptions":{"MaxPrice":"'"$SPOT_MAX_PRICE"'","SpotInstanceType":"persistent","InstanceInterruptionBehavior":"stop"}}'
    fi

    INSTANCE_ID=$(aws ec2 run-instances \
        --region "$REGION" \
        --image-id "$AMI_ID" \
        --instance-type "$INSTANCE_TYPE" \
        --key-name "$KEY_NAME" \
        --security-group-ids "$SG_ID" \
        --iam-instance-profile Name="$PROFILE_NAME" \
        ${MARKET_OPTIONS:+$MARKET_OPTIONS} \
        --block-device-mappings '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":'"$EBS_SIZE"',"VolumeType":"gp3","Iops":6000,"Throughput":400}}]' \
        --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value='"$RUNNER_TAG"'},{Key=Project,Value=polytope-classifier}]' \
        --user-data "$USERDATA" \
        --query 'Instances[0].InstanceId' --output text)

    INSTANCE_IDS+=("$INSTANCE_ID")
    echo "  ✓ Launched: $INSTANCE_ID ($RUNNER_TAG)"
done

echo ""

# ═══════════════════════════════════════════════════════════════════════════
# STEP 8: Wait for instances and print connection info
# ═══════════════════════════════════════════════════════════════════════════

echo "=== Step 8: Waiting for instances to start ==="

aws ec2 wait instance-running \
    --region "$REGION" \
    --instance-ids "${INSTANCE_IDS[@]}"

echo ""
echo "═══════════════════════════════════════════════════════════════"
echo " All instances running!"
echo "═══════════════════════════════════════════════════════════════"
echo ""

for i in $(seq 0 $(( NUM_INSTANCES - 1 ))); do
    ID="${INSTANCE_IDS[$i]}"
    START=$(( FILE_START + i * FILES_PER_INSTANCE ))
    END=$(( START + FILES_PER_INSTANCE - 1 ))
    (( i == NUM_INSTANCES - 1 )) && END=$FILE_END

    IP=$(aws ec2 describe-instances --region "$REGION" \
        --instance-ids "$ID" \
        --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)

    echo "  Runner $i  [$ID]"
    echo "    Files: $START–$END"
    echo "    IP:    $IP"
    echo "    SSH:   ssh -i ${KEY_NAME}.pem ubuntu@$IP"
    echo "    Log:   ssh -i ${KEY_NAME}.pem ubuntu@$IP 'tail -f /tmp/polytope-output/run.log'"
    echo ""
done

# Save instance info for monitoring
cat > /tmp/polytope-fleet-info.sh <<EOF
#!/usr/bin/env bash
# Polytope fleet info — generated $(date)
REGION="$REGION"
BUCKET_NAME="$BUCKET_NAME"
KEY_NAME="$KEY_NAME"
INSTANCE_IDS=(${INSTANCE_IDS[*]})
NUM_INSTANCES=$NUM_INSTANCES
FILE_START=$FILE_START
FILE_END=$FILE_END
EOF

echo "═══════════════════════════════════════════════════════════════"
echo " Fleet info saved to /tmp/polytope-fleet-info.sh"
echo ""
echo " Monitor commands:"
echo "   # Check S3 checkpoints:"
echo "   aws s3 ls s3://$BUCKET_NAME/checkpoints/ --region $REGION"
echo ""
echo "   # SSH to any runner:"
echo "   ssh -i ${KEY_NAME}.pem ubuntu@<IP> 'tail -20 /tmp/polytope-output/run.log'"
echo ""
echo "   # Check all instance status:"
echo "   aws ec2 describe-instances --region $REGION \\"
echo "     --filters 'Name=tag:Project,Values=polytope-classifier' \\"
echo "     --query 'Reservations[].Instances[].[InstanceId,State.Name,PublicIpAddress]' \\"
echo "     --output table"
echo ""
echo "   # Terminate all when done:"
echo "   aws ec2 terminate-instances --region $REGION --instance-ids ${INSTANCE_IDS[*]}"
echo "═══════════════════════════════════════════════════════════════"

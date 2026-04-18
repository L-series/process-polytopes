#!/usr/bin/env bash
# monitor_fleet.sh — Monitor and manage the EC2 polytope fleet
#
# Usage:
#   ./scripts/monitor_fleet.sh status      # instance status
#   ./scripts/monitor_fleet.sh checkpoints  # list S3 checkpoints
#   ./scripts/monitor_fleet.sh logs         # tail logs from all runners
#   ./scripts/monitor_fleet.sh cost         # estimate cost so far
#   ./scripts/monitor_fleet.sh terminate    # terminate all instances
#
# ═══════════════════════════════════════════════════════════════════════════
set -euo pipefail

# Load fleet info if available
if [[ -f /tmp/polytope-fleet-info.sh ]]; then
    source /tmp/polytope-fleet-info.sh
else
    REGION="${REGION:-us-east-1}"
    BUCKET_NAME="${BUCKET_NAME:-}"
    KEY_NAME="${KEY_NAME:-polytope-key}"
fi

CMD="${1:-status}"

case "$CMD" in

status)
    echo "=== Instance Status ==="
    aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Project,Values=polytope-classifier" \
        --query 'Reservations[].Instances[].[
            Tags[?Key==`Name`].Value | [0],
            InstanceId,
            State.Name,
            InstanceType,
            PublicIpAddress,
            LaunchTime
        ]' --output table
    ;;

checkpoints)
    echo "=== S3 Checkpoints ==="
    if [[ -n "$BUCKET_NAME" ]]; then
        aws s3 ls "s3://$BUCKET_NAME/checkpoints/" --region "$REGION" \
            --human-readable --summarize
    else
        echo "Error: BUCKET_NAME not set. Source /tmp/polytope-fleet-info.sh or set it."
    fi
    ;;

logs)
    echo "=== Tailing logs from all runners ==="
    IPS=$(aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Project,Values=polytope-classifier" "Name=instance-state-name,Values=running" \
        --query 'Reservations[].Instances[].PublicIpAddress' --output text)

    for IP in $IPS; do
        echo ""
        echo "--- $IP ---"
        ssh -i "${KEY_NAME}.pem" -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
            "ubuntu@$IP" 'tail -20 /tmp/polytope-output/run.log 2>/dev/null || echo "Log not ready yet"' \
            2>/dev/null || echo "  (unreachable)"
    done
    ;;

progress)
    echo "=== Progress per runner ==="
    IPS=$(aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Project,Values=polytope-classifier" "Name=instance-state-name,Values=running" \
        --query 'Reservations[].Instances[].[Tags[?Key==`Name`].Value | [0], PublicIpAddress]' --output text)

    while IFS=$'\t' read -r NAME IP; do
        echo ""
        echo "--- $NAME ($IP) ---"
        ssh -i "${KEY_NAME}.pem" -o ConnectTimeout=5 -o StrictHostKeyChecking=no \
            "ubuntu@$IP" '
            LOG=/tmp/polytope-output/run.log
            if [[ -f "$LOG" ]]; then
                grep -E "Batch [0-9]+ / [0-9]+" "$LOG" | tail -3
                grep -E "ETA:|Elapsed:" "$LOG" | tail -1
            else
                echo "Log not ready yet"
            fi
        ' 2>/dev/null || echo "  (unreachable)"
    done <<< "$IPS"
    ;;

cost)
    echo "=== Estimated Cost ==="
    # Get running hours per instance
    aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Project,Values=polytope-classifier" \
        --query 'Reservations[].Instances[].[
            Tags[?Key==`Name`].Value | [0],
            InstanceId,
            State.Name,
            LaunchTime
        ]' --output text | while IFS=$'\t' read -r NAME ID STATE LAUNCH; do
        if [[ -n "$LAUNCH" ]]; then
            LAUNCH_EPOCH=$(date -d "$LAUNCH" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%S" "$LAUNCH" +%s 2>/dev/null || echo 0)
            NOW_EPOCH=$(date +%s)
            HOURS=$(( (NOW_EPOCH - LAUNCH_EPOCH) / 3600 ))
            # Approximate spot cost — check actual price with 'aws ec2 describe-spot-price-history'
            COST=$(echo "$HOURS * 0.75" | bc 2>/dev/null || echo "~")
            echo "  $NAME ($ID): ${HOURS}h uptime ≈ \$$COST"
        fi
    done

    if [[ -n "$BUCKET_NAME" ]]; then
        echo ""
        echo "  S3 storage:"
        aws s3 ls "s3://$BUCKET_NAME/checkpoints/" --region "$REGION" \
            --summarize --human-readable 2>/dev/null | tail -2
    fi
    ;;

terminate)
    echo "=== Terminating all polytope instances ==="
    IDS=$(aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Project,Values=polytope-classifier" "Name=instance-state-name,Values=running,stopped" \
        --query 'Reservations[].Instances[].InstanceId' --output text)

    if [[ -z "$IDS" ]]; then
        echo "  No running instances found."
    else
        echo "  Terminating: $IDS"
        read -p "  Are you sure? (y/N) " -r
        if [[ "$REPLY" =~ ^[Yy]$ ]]; then
            aws ec2 terminate-instances --region "$REGION" --instance-ids $IDS
            echo "  ✓ Termination initiated"
        else
            echo "  Cancelled."
        fi
    fi
    ;;

ssh)
    # Quick SSH to runner N: ./monitor_fleet.sh ssh 0
    IDX="${2:-0}"
    IP=$(aws ec2 describe-instances --region "$REGION" \
        --filters "Name=tag:Project,Values=polytope-classifier" "Name=instance-state-name,Values=running" \
        --query "Reservations[].Instances[].PublicIpAddress" --output text | sed -n "$((IDX+1))p")
    if [[ -n "$IP" ]]; then
        echo "Connecting to runner $IDX at $IP..."
        ssh -i "${KEY_NAME}.pem" -o StrictHostKeyChecking=no "ubuntu@$IP"
    else
        echo "Runner $IDX not found or not running."
    fi
    ;;

*)
    echo "Usage: $0 {status|checkpoints|logs|progress|cost|terminate|ssh [N]}"
    ;;

esac

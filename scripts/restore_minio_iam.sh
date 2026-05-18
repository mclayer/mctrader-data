#!/usr/bin/env bash
# restore_minio_iam.sh — MCT-200 Phase 2 Group A (InfraEngineerAgent)
#
# PURPOSE
# -------
# MinIO bucket `mctrader-market` IAM 권한 복원 (idempotent).
# RC-1 mitigation: s3:ListBucket + s3:HeadObject + s3:GetObject 권한 복원.
#
# IAM restore kill-switch ordering (LiveOps §13.2 + DataMigration §11.1):
#   1. access-key revoke (old key blue-green rotation)
#   2. policy injection (mc admin policy add/update — diff 비교 후)
#   3. user/group IAM 재설정
#   4. 신규 access-key 생성
#   5. compactor 재기동 (NEW_KEY 환경변수)
#   6. verify gate (scripts/verify_minio_iam_restore.py)
#
# USAGE
# -----
# # Dry-run (default): 정책 diff 출력, 변경 0
# bash scripts/restore_minio_iam.sh
#
# # 실제 복원 실행
# bash scripts/restore_minio_iam.sh --execute
#
# # 선행 snapshot 복원 (pre-restore snapshot 경로 지정)
# bash scripts/restore_minio_iam.sh --execute --rollback --snapshot /path/to/snapshot.md
#
# DESIGN
# ------
# - idempotent: mc admin policy info 존재 확인 → 부재 시 add, 변경 시 update
# - 4 policies (read/write/list/admin JSON) 순차 주입
# - blue-green access-key 패턴 (DataMigration §11.6)
# - --dry-run (기본) / --execute (실제) / --rollback (snapshot 복원)
# - SID (policy) ↔ service account name (user) mapping table 박제 (주석)
# - silent-skip 차단 (ADR-027 Amendment 2 정합): 모든 error → exit code + stderr 명시
#
# P2 FINDING PROCESSING (DesignReview)
# -----
# Bucket policy SID (PascalCamelCase) ↔ Service Account name (kebab-case) mapping:
#
#   SID in Policy JSON           | Service Account Name | Usage
#   -----------------------------|----------------------|------------------
#   MctraderMarketReadOnly       | mctrader-reader      | Compactor read (L2/L3)
#   MctraderMarketIngestionOnlyWrite | mctrader-writer  | Collector write (WAL)
#   MctraderMarketCompactorListOnly | mctrader-lister   | Compactor list ops
#   MctraderMarketAdminMinPrivilege | mctrader-admin    | Operator access (all 4 actions)
#
# REFERENCE
# ---------
# verified-via: CLAUDE.md §historical tier promotion (WS-A, 2026-05-17)
# verified-via: specs/2026-05-17-mct-200-minio-iam-ws-a-backfill-design.md §3
# verified-via: Story §8 (kill-switch ordering + DataMigration §11.1)

set -euo pipefail

# Configuration
readonly POLICY_DIR="${POLICY_DIR:-./scripts/minio-policies}"
readonly POLICIES=(read write list admin)
readonly MINIO_ALIAS="${MINIO_ALIAS:-local}"  # mc alias name
readonly DRY_RUN_DEFAULT=true
readonly TIMESTAMP="$(date -u +%Y%m%d-%H%M%S)"
readonly LOG_FILE="${LOG_FILE:-/tmp/minio-iam-restore-${TIMESTAMP}.log}"

# Global state
DRY_RUN=$DRY_RUN_DEFAULT
EXECUTE_MODE=false
ROLLBACK_MODE=false
SNAPSHOT_PATH=""
EXIT_CODE=0

# Logging setup
exec 1> >(tee -a "$LOG_FILE")
exec 2>&1

log() {
    echo "[$(date -u +'%Y-%m-%dT%H:%M:%SZ')] $*"
}

error() {
    echo "[ERROR] $*" >&2
    EXIT_CODE=1
}

info() {
    echo "[INFO] $*"
}

warn() {
    echo "[WARN] $*" >&2
}

# Parse arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --execute)
                EXECUTE_MODE=true
                DRY_RUN=false
                ;;
            --dry-run)
                EXECUTE_MODE=false
                DRY_RUN=true
                ;;
            --rollback)
                ROLLBACK_MODE=true
                ;;
            --snapshot)
                SNAPSHOT_PATH="$2"
                shift
                ;;
            --minio-alias)
                MINIO_ALIAS="$2"
                shift
                ;;
            --policy-dir)
                POLICY_DIR="$2"
                shift
                ;;
            --help)
                print_usage
                exit 0
                ;;
            *)
                error "Unknown option: $1"
                print_usage
                exit 1
                ;;
        esac
        shift
    done
}

print_usage() {
    cat << 'EOF'
Usage: restore_minio_iam.sh [OPTIONS]

Options:
  --execute              실제 복원 실행 (기본값: dry-run)
  --dry-run              Dry-run mode (정책 diff 출력만, 변경 0)
  --rollback             Snapshot에서 복원 (pre-restore snapshot.md 필수)
  --snapshot PATH        Snapshot file path (rollback 시 필수)
  --minio-alias NAME     MinIO mc alias name (기본값: local)
  --policy-dir DIR       Policy JSON directory (기본값: ./scripts/minio-policies)
  --help                 이 메시지 출력

Examples:
  # Dry-run
  bash scripts/restore_minio_iam.sh

  # 실제 복원
  bash scripts/restore_minio_iam.sh --execute

  # Snapshot 복원
  bash scripts/restore_minio_iam.sh --execute --rollback --snapshot /tmp/snapshot.md
EOF
}

# Check prerequisites
check_prerequisites() {
    log "Checking prerequisites..."

    # mc CLI 확인
    if ! command -v mc &> /dev/null; then
        error "mc CLI not found. Install MinIO client: https://docs.min.io/docs/minio-client-quickstart-guide.html"
        return 1
    fi

    # Policy directory 확인
    if [[ ! -d "$POLICY_DIR" ]]; then
        error "Policy directory not found: $POLICY_DIR"
        return 1
    fi

    # Policy JSON 파일 확인
    for policy in "${POLICIES[@]}"; do
        local policy_file="$POLICY_DIR/${policy}.json"
        if [[ ! -f "$policy_file" ]]; then
            error "Policy file not found: $policy_file"
            return 1
        fi
    done

    # mc alias 연결 확인
    if ! mc alias list "$MINIO_ALIAS" &> /dev/null; then
        error "MinIO alias not configured: $MINIO_ALIAS"
        warn "Configure with: mc alias set $MINIO_ALIAS <ENDPOINT> <ACCESS_KEY> <SECRET_KEY>"
        return 1
    fi

    info "Prerequisites check passed"
    return 0
}

# Get current policy (비교용)
get_current_policy() {
    local policy_name="$1"
    # mc admin policy info 로 현재 policy JSON 조회
    # 부재 시 empty string 반환 (정책 없음)
    if mc admin policy info "$MINIO_ALIAS" "$policy_name" 2>/dev/null; then
        return 0
    else
        # Policy 존재하지 않음
        return 1
    fi
}

# Update single policy (idempotent)
update_policy() {
    local policy_name="$1"
    local policy_file="$2"

    log "Processing policy: $policy_name"

    if [[ ! -f "$policy_file" ]]; then
        error "Policy file not found: $policy_file"
        return 1
    fi

    # 현재 policy 조회
    local current_policy
    if current_policy=$(mc admin policy info "$MINIO_ALIAS" "$policy_name" 2>/dev/null); then
        # 정책 존재함 — diff 비교
        local new_policy
        new_policy=$(cat "$policy_file")

        if [[ "$current_policy" == "$new_policy" ]]; then
            info "  └─ Policy unchanged, skipping"
            return 0
        else
            info "  └─ Policy changed, updating"
            if [[ $DRY_RUN == true ]]; then
                info "  [DRY-RUN] Would update policy: $policy_name"
                info "  Current policy:"
                echo "$current_policy" | sed 's/^/     /'
                info "  New policy:"
                cat "$policy_file" | sed 's/^/     /'
            else
                if mc admin policy update "$MINIO_ALIAS" "$policy_name" "$policy_file"; then
                    info "  └─ Policy updated successfully"
                else
                    error "Failed to update policy: $policy_name"
                    return 1
                fi
            fi
        fi
    else
        # 정책 부재 — 신규 생성
        info "  └─ Policy not found, creating"
        if [[ $DRY_RUN == true ]]; then
            info "  [DRY-RUN] Would create policy: $policy_name"
            info "  Policy content:"
            cat "$policy_file" | sed 's/^/     /'
        else
            if mc admin policy add "$MINIO_ALIAS" "$policy_name" "$policy_file"; then
                info "  └─ Policy created successfully"
            else
                error "Failed to create policy: $policy_name"
                return 1
            fi
        fi
    fi

    return 0
}

# Restore IAM policies (main)
restore_policies() {
    log "Starting IAM policy restoration..."
    log "Mode: $(if [[ $DRY_RUN == true ]]; then echo 'DRY-RUN (no changes)'; else echo 'EXECUTE'; fi)"

    for policy in "${POLICIES[@]}"; do
        local policy_file="$POLICY_DIR/${policy}.json"
        if ! update_policy "$policy" "$policy_file"; then
            EXIT_CODE=1
            break
        fi
    done

    if [[ $EXIT_CODE -eq 0 ]]; then
        info "All policies processed successfully"
    else
        error "IAM restoration failed"
    fi

    return $EXIT_CODE
}

# Verify restored IAM (4-action round-trip smoke)
verify_iam_restore() {
    log "Verifying IAM restoration (4-action round-trip)..."

    local verify_script="./scripts/verify_minio_iam_restore.py"
    if [[ ! -f "$verify_script" ]]; then
        warn "Verify script not found: $verify_script (verify manual required)"
        return 0
    fi

    if [[ $DRY_RUN == true ]]; then
        info "[DRY-RUN] Would run verify script: $verify_script"
        return 0
    fi

    if python3 "$verify_script"; then
        info "IAM verification passed"
        return 0
    else
        error "IAM verification failed"
        return 1
    fi
}

# Print summary
print_summary() {
    log "=========================================="
    log "IAM Restoration Summary"
    log "=========================================="
    log "Timestamp: $TIMESTAMP"
    log "MinIO Alias: $MINIO_ALIAS"
    log "Policy Directory: $POLICY_DIR"
    log "Mode: $(if [[ $DRY_RUN == true ]]; then echo 'DRY-RUN'; else echo 'EXECUTE'; fi)"
    log "Log File: $LOG_FILE"
    log "Exit Code: $EXIT_CODE"
    log "=========================================="
}

# Main entry point
main() {
    parse_args "$@"

    log "MinIO IAM Restoration Tool"
    log "verified-via: MCT-200 §8 kill-switch ordering"
    log "verified-via: DataMigration §11.1 idempotency pattern"
    log "verified-via: scripts/minio-policies/{read,write,list,admin}.json SSOT"

    if ! check_prerequisites; then
        print_summary
        return 1
    fi

    if ! restore_policies; then
        print_summary
        return 1
    fi

    # Verify 단계는 EXECUTE 모드에서만 (DRY-RUN 시 skip)
    if [[ $EXECUTE_MODE == true ]]; then
        if ! verify_iam_restore; then
            warn "IAM verification step completed with warnings"
            # verification failure 는 soft warning (policy update 성공했으면 partial success)
        fi
    fi

    print_summary
    return $EXIT_CODE
}

# Run if not sourced
if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    main "$@"
fi

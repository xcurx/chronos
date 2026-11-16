package com.minidb.grpc;

import com.google.protobuf.ByteString;
import com.minidb.proto.CommandType;
import com.minidb.proto.LogEntry;
import com.minidb.raft.RaftLog;

/**
 * Shared bidirectional mapper between {@link RaftLog.Entry} and protobuf {@link LogEntry}.
 */
public final class RaftProtoMapper {

    private RaftProtoMapper() {}

    public static CommandType toProto(RaftLog.CommandType type) {
        if (type == null) {
            return CommandType.NOOP;
        }
        return switch (type) {
            case PUT -> CommandType.PUT;
            case DELETE -> CommandType.DELETE;
            case TXN_PREPARE -> CommandType.TXN_PREPARE;
            case TXN_COMMIT -> CommandType.TXN_COMMIT;
            case TXN_ABORT -> CommandType.TXN_ABORT;
            case CHRONOS_WRITE -> CommandType.CHRONOS_WRITE;
            case CHRONOS_TXN -> CommandType.CHRONOS_TXN;
            case CHRONOS_BRANCH_WRITE -> CommandType.CHRONOS_BRANCH_WRITE;
            case CHRONOS_MERGE -> CommandType.CHRONOS_MERGE;
            case NOOP -> CommandType.NOOP;
        };
    }

    public static RaftLog.CommandType fromProto(CommandType type) {
        if (type == null) {
            return RaftLog.CommandType.NOOP;
        }
        return switch (type) {
            case PUT -> RaftLog.CommandType.PUT;
            case DELETE -> RaftLog.CommandType.DELETE;
            case TXN_PREPARE -> RaftLog.CommandType.TXN_PREPARE;
            case TXN_COMMIT -> RaftLog.CommandType.TXN_COMMIT;
            case TXN_ABORT -> RaftLog.CommandType.TXN_ABORT;
            case CHRONOS_WRITE -> RaftLog.CommandType.CHRONOS_WRITE;
            case CHRONOS_TXN -> RaftLog.CommandType.CHRONOS_TXN;
            case CHRONOS_BRANCH_WRITE -> RaftLog.CommandType.CHRONOS_BRANCH_WRITE;
            case CHRONOS_MERGE -> RaftLog.CommandType.CHRONOS_MERGE;
            default -> RaftLog.CommandType.NOOP;
        };
    }

    public static LogEntry toProtoEntry(RaftLog.Entry entry) {
        LogEntry.Builder builder = LogEntry.newBuilder()
                .setIndex(entry.index())
                .setTerm(entry.term())
                .setCommandType(toProto(entry.type()))
                .setKey(entry.key() != null ? entry.key() : "")
                .setTimestamp(entry.timestamp())
                .setVersionId(entry.versionId() != null ? entry.versionId() : "")
                .setVersionSequence(entry.versionSequence())
                .setBranchId(entry.branchId() != null ? entry.branchId() : "main")
                .setWriterId(entry.writerId() != null ? entry.writerId() : "");

        if (entry.value() != null) {
            builder.setValue(ByteString.copyFrom(entry.value()));
        }
        if (entry.parentVersionIds() != null && !entry.parentVersionIds().isEmpty()) {
            builder.addAllParentVersionIds(entry.parentVersionIds());
        }
        return builder.build();
    }

    public static RaftLog.Entry fromProtoEntry(LogEntry protoEntry) {
        RaftLog.CommandType cmdType = fromProto(protoEntry.getCommandType());
        String key = protoEntry.getKey().isEmpty() ? null : protoEntry.getKey();
        byte[] value = (protoEntry.getValue().isEmpty()
                && (key == null || cmdType == RaftLog.CommandType.DELETE || cmdType == RaftLog.CommandType.NOOP))
                ? null
                : protoEntry.getValue().toByteArray();
        String versionId = protoEntry.getVersionId().isEmpty() ? null : protoEntry.getVersionId();
        String branchId = protoEntry.getBranchId().isEmpty() ? "main" : protoEntry.getBranchId();
        String writerId = protoEntry.getWriterId().isEmpty() ? null : protoEntry.getWriterId();

        return new RaftLog.Entry(
                protoEntry.getIndex(),
                protoEntry.getTerm(),
                cmdType,
                key,
                value,
                protoEntry.getTimestamp(),
                versionId,
                protoEntry.getVersionSequence(),
                branchId,
                writerId,
                protoEntry.getParentVersionIdsList()
        );
    }
}

public class TransactionalMsgResult {
    private final long commitedMessages;
    private final long unCommittedMessages;

    public TransactionalMsgResult(long commitedMessages, long unCommittedMessages) {
        this.commitedMessages = commitedMessages;
        this.unCommittedMessages = unCommittedMessages;
    }

    public long getCommitedMessages() {
        return commitedMessages;
    }

    public long getUnCommittedMessages() {
        return unCommittedMessages;
    }
}

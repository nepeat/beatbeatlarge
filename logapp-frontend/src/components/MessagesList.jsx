import './MessageList.css';

export default (props) => {
    let messages = props.messages;

    if (!messages || messages.length === 0) {
        return <p>No messages!</p>;
    }

    return <div className="logs">
        {messages.map(message => {
        return (
            <p>[{message.host}] {message.message}</p>
        );
        })}
    </div>
};
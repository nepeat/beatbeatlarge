import { Component } from 'react';

import Header from './Header';
import MessagesList from './MessagesList';

class MessagesContainer extends Component {
    constructor(props) {
        super(props);
        this.state = {
            messages: []
        };
    }

    onMessage = (message) => {
        let newMessages = [...this.state.messages, message];

        if (newMessages.length > 100) newMessages.shift();

        this.setState({
            messages: newMessages
        });

        // if (this.state.messages.length > 1000) this.state.messages.pop();
    }

    componentDidMount() {
        this.props.socket.on("message", this.onMessage);
    }

    componentWillUnmount() {
        this.props.socket.off("message", this.onMessage);
    }

    render() {
        return <div>
            <MessagesList messages={this.state.messages}/>
        </div>;
    }
}

export default MessagesContainer;

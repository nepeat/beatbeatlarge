import { Component } from 'react';

import Header from './Header';
import MessagesContainer from './MessagesContainer';

class Container extends Component {
    render() {
        return <div className="App">
            <Header/>
            <MessagesContainer socket={this.props.socket}/>
        </div>;
    }
}

export default Container;

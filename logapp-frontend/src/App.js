import { io } from "socket.io-client";

import logo from './logo.svg';
import './App.css';

import Container from './components/Container';

const socket = io();

function App() {
  return <Container socket={socket}/>;
}

export default App;

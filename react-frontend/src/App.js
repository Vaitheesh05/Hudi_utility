import React from 'react';
import { BrowserRouter as Router, Route, Routes, Link } from 'react-router-dom';
import BootstrapPage from './BootstrapPage';
import HistoryTable from './HistoryTable';
import { AppBar, Toolbar, Button, Typography, Container } from '@mui/material';

function App() {
    return (
        <Router>
            <AppBar position="static">
                <Toolbar>
                    <Typography variant="h6" sx={{ flexGrow: 1 }}>
                        Hudi Bootstrap
                    </Typography>
                    <Button color="inherit" component={Link} to="/">History</Button>
                    <Button color="inherit" component={Link} to="/bootstrap">New Bootstrapping Request</Button>
                </Toolbar>
            </AppBar>
            <Container>
                <Routes>
                    <Route path="/" element={<HistoryTable />} />
                    <Route path="/bootstrap" element={<BootstrapPage />} />
                </Routes>
            </Container>
        </Router>
    );
}

export default App;


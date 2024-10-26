import React, { useState } from 'react';
import { BrowserRouter as Router, Route, Routes, Link } from 'react-router-dom';
import BootstrapPage from './BootstrapPage';
import HistoryTable from './HistoryTable';
import { AppBar, Toolbar, Button, Typography, Container } from '@mui/material';
import { ThemeProvider } from '@mui/material/styles';
import AddIcon from '@mui/icons-material/Add'; 
import theme from './theme';  // Import the custom theme

function App() {
    const [reset, setReset] = useState(false);

    const handleNewClick = () => {
        setReset((prev) => !prev); // Toggle the reset state
    };

    return (
        <ThemeProvider theme={theme}>
            <Router>
                <AppBar position="static" sx={{
                    borderBottom: '1px solid var(--mui-palette-divider)',
                    backgroundColor: '#212636',
                    position: 'sticky',
                    top: 0,
                    zIndex: 'var(--mui-zIndex-appBar)',
                    color: '#ffffff',
                }}>
                    <Toolbar>
                        <Typography variant="h6" sx={{ flexGrow: 1 }}>
                            Hudi Bootstrap
                        </Typography>
                        <Button color="inherit" component={Link} to="/" sx={{ margin: '0 1vw', padding: "8px 16px", }}>History</Button>
                        <Button color="inherit" component={Link} to="/bootstrap" onClick={handleNewClick} startIcon={<AddIcon />} sx={{ display: 'flex', alignItems: 'center', backgroundColor: "#635bff", padding: "8px 16px", }}>New</Button>
                    </Toolbar>
                </AppBar>
                <Container maxWidth={false} sx={{ padding: 2 }}>
                    <Routes>
                        <Route path="/" element={<HistoryTable />} />
                        <Route path="/bootstrap" element={<BootstrapPage reset={reset} />} />
                    </Routes>
                </Container>
            </Router>
        </ThemeProvider>
    );
}

export default App;

import React, { useEffect, useState } from 'react';
import { Table, TableHead, TableRow, TableCell, TableBody, Button, TextField, Box, Typography } from '@mui/material';
import axios from 'axios';
import { useNavigate } from 'react-router-dom';

function HistoryTable() {
    const [history, setHistory] = useState([]);
    const [searchTerm, setSearchTerm] = useState('');
    const [startDate, setStartDate] = useState('');
    const [endDate, setEndDate] = useState('');
    const [loading, setLoading] = useState(false);
    const navigate = useNavigate();

    useEffect(() => {
        fetchHistory();
    }, []);

    const fetchHistory = async (startDate = '', endDate = '', transactionId = '') => {
	    setLoading(true);
	    try {
		const params = { start_date: startDate, end_date: endDate, transaction_id: transactionId };
		const response = await axios.get('http://127.0.0.1:8000/bootstrap_history/', { params });
		setHistory(response.data);
	    } catch (error) {
		console.error("Error fetching history:", error);
		//setMessage({ text: "Failed to fetch history.", severity: 'error' });
	    } finally {
		setLoading(false);
	    }
	};


    const handleSearch = () => {
        fetchHistory(startDate, endDate, searchTerm);
    };

    const handleRerun = (transaction) => {
        const formData = JSON.parse(transaction.transaction_data);
        navigate('/bootstrap', { state: { formData } });
    };

    return (
        <Box>
            <Box sx={{ display: 'flex', justifyContent: 'space-between', mb: 2 }}>
                <Box>
                    <TextField label="Search by Transaction ID" value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} />
                </Box>
                <Box>
                    <TextField type="date" label="Start Date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
                    <TextField type="date" label="End Date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
                    <Button variant="contained" onClick={handleSearch}>Fetch</Button>
                </Box>
            </Box>
            {loading ? (
                <Typography>Loading...</Typography>
            ) : (
                <Table>
                    <TableHead>
                        <TableRow>
                            <TableCell>Transaction ID</TableCell>
                            <TableCell>Job ID</TableCell>
                            <TableCell>Status</TableCell>
                            <TableCell>Start Time</TableCell>
                            <TableCell>End Time</TableCell>
                            <TableCell>Action</TableCell>
                        </TableRow>
                    </TableHead>
                    <TableBody>
                        {history.map((transaction) => (
                            <TableRow key={transaction.transaction_id}>
                                <TableCell>{transaction.transaction_id}</TableCell>
                                <TableCell>{transaction.job_id}</TableCell>
                                <TableCell>{transaction.status}</TableCell>
                                <TableCell>{new Date(transaction.start_time).toLocaleString()}</TableCell>
                                <TableCell>{transaction.end_time ? new Date(transaction.end_time).toLocaleString() : 'In Progress'}</TableCell>
                                <TableCell>
                                    <Button onClick={() => handleRerun(transaction)}>Rerun</Button>
                                </TableCell>
                            </TableRow>
                        ))}
                    </TableBody>
                </Table>
            )}
        </Box>
    );
}

export default HistoryTable;

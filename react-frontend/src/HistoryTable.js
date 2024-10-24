import React, { useEffect, useState } from 'react';
import { Table, TableHead, TableRow, TableCell, TableBody, Button } from '@mui/material';
import axios from 'axios';

function HistoryTable({ setFormData, setLog, handleOpenLog }) {
    const [history, setHistory] = useState([]);

    useEffect(() => {
        async function fetchHistory() {
            const response = await axios.get('http://127.0.0.1:8000/bootstrap_history/');
            setHistory(response.data);
        }
        fetchHistory();
    }, []);

    const handleRerun = (transaction) => {
        setFormData(JSON.parse(transaction.transaction_data));  // Load data back to form
    };

    const handleShowLog = (log) => {
        setLog(log);  // Set the log data to be displayed
        handleOpenLog();  // Open the log modal
    };
    return (
        <Table>
            <TableHead>
                <TableRow>
                    <TableCell>Transaction ID</TableCell>
                    <TableCell>Status</TableCell>
                    <TableCell>Start Time</TableCell>
                    <TableCell>End Time</TableCell>
                    <TableCell>Action</TableCell>
		    <TableCell>View Logs</TableCell>
                </TableRow>
            </TableHead>
            <TableBody>
                {history.map((transaction) => (
                    <TableRow key={transaction.transaction_id}>
                        <TableCell>{transaction.transaction_id}</TableCell>
                        <TableCell>{transaction.status}</TableCell>
                        <TableCell>{new Date(transaction.start_time).toLocaleString()}</TableCell>
                        <TableCell>{transaction.end_time ? new Date(transaction.end_time).toLocaleString() : 'In Progress'}</TableCell>
                        <TableCell>
                            <Button onClick={() => handleRerun(transaction)}>Rerun</Button>
                        </TableCell>
			<TableCell>
			    <Button onClick={() => handleShowLog(transaction.log)}>Show Log</Button>
			</TableCell>
                    </TableRow>
                ))}
            </TableBody>
        </Table>
    );
}

export default HistoryTable;

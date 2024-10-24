import React, { useState } from 'react';
import axios from 'axios';
import { TextField, Button, MenuItem, FormControl, Select, InputLabel, Checkbox, FormControlLabel, CircularProgress, Typography, Box, Alert } from '@mui/material';
import HistoryTable from './HistoryTable';
import TransactionLogModal from './TransactionLogModal';

function App() {
    const [formData, setFormData] = useState({
        data_file_path: "",
        hudi_table_name: "",
        key_field: "",
        precombine_field: "",
        partition_field: "",
        hudi_table_type: "COPY_ON_WRITE",
        write_operation: "insert",
        output_path: "",
        spark_config: { 'spark.executor.memory': '2g' }, // Ensure this is correctly initialized
        schema_validation: false,
        dry_run: false,
        bootstrap_type: "FULL_RECORD",
        partition_regex: ""
    });

    const [loading, setLoading] = useState(false);
    const [log, setLog] = useState('');  // State to store the transaction log
    const [logModalOpen, setLogModalOpen] = useState(false);  // State to control modal visibility
    
    const [message, setMessage] = useState({ text: '', severity: '' });

    const handleChange = (e) => {
        const { name, value, type, checked } = e.target;

        if (type === 'checkbox') {
            setFormData((prevFormData) => ({
                ...prevFormData,
                [name]: checked
            }));
        } else {
            setFormData((prevFormData) => ({
                ...prevFormData,
                [name]: value
            }));
        }
    };

    const handleSubmit = async (e) => {
        e.preventDefault();

        // Flatten spark_config, ensuring it is JSON formatted
        const formattedData = {
            ...formData,
            spark_config: formData.spark_config, // This is already an object, no need to stringify
        };

        setLoading(true);
        try {
            const response = await axios.post(
                "http://127.0.0.1:8000/bootstrap_hudi/",
                formattedData,
                {
                    headers: {
                        'Content-Type': 'application/json',
                    }
                }
            );

            if (response.status === 200) {
                setMessage({ text: response.data.message, severity: 'success' });
            }
        } catch (error) {
            console.error("There was an error bootstrapping the Hudi table!", error);
            setMessage({ text: "Failed to bootstrap the Hudi table.", severity: 'error' });
        }
        setLoading(false);
    };

    const handleOpenLog = () => {
        setLogModalOpen(true);  // Open the modal
    };

    const handleCloseLog = () => {
        setLogModalOpen(false);  // Close the modal
    };

    return (
	<>
        <Box sx={{ maxWidth: 600, margin: 'auto', padding: 2 }}>
            <Typography variant="h4" gutterBottom>
                Bootstrap Hudi Table
            </Typography>
            <form onSubmit={handleSubmit}>
                <TextField
                    label="Data File Path"
                    name="data_file_path"
                    value={formData.data_file_path}
                    onChange={handleChange}
                    fullWidth
                    margin="normal"
                    required
                />
                <TextField
                    label="Hudi Table Name"
                    name="hudi_table_name"
                    value={formData.hudi_table_name}
                    onChange={handleChange}
                    fullWidth
                    margin="normal"
                    required
                />
                <TextField
                    label="Key Field"
                    name="key_field"
                    value={formData.key_field}
                    onChange={handleChange}
                    fullWidth
                    margin="normal"
                    required
                />
                <TextField
                    label="Precombine Field"
                    name="precombine_field"
                    value={formData.precombine_field}
                    onChange={handleChange}
                    fullWidth
                    margin="normal"
                    required
                />
                <TextField
                    label="Partition Field"
                    name="partition_field"
                    value={formData.partition_field}
                    onChange={handleChange}
                    fullWidth
                    margin="normal"
                    required
                />

                <FormControl fullWidth margin="normal" required>
                    <InputLabel>Hudi Table Type</InputLabel>
                    <Select
                        name="hudi_table_type"
                        value={formData.hudi_table_type}
                        onChange={handleChange}
                    >
                        <MenuItem value="COPY_ON_WRITE">COPY_ON_WRITE</MenuItem>
                        <MenuItem value="MERGE_ON_READ">MERGE_ON_READ</MenuItem>
                    </Select>
                </FormControl>

                <FormControl fullWidth margin="normal" required>
                    <InputLabel>Write Operation</InputLabel>
                    <Select
                        name="write_operation"
                        value={formData.write_operation}
                        onChange={handleChange}
                    >
                        <MenuItem value="insert">Insert</MenuItem>
                        <MenuItem value="upsert">Upsert</MenuItem>
                    </Select>
                </FormControl>

                <TextField
                    label="Output Path"
                    name="output_path"
                    value={formData.output_path}
                    onChange={handleChange}
                    fullWidth
                    margin="normal"
                    required
                />

                <FormControl fullWidth margin="normal" required>
                    <InputLabel>Bootstrap Type</InputLabel>
                    <Select
                        name="bootstrap_type"
                        value={formData.bootstrap_type}
                        onChange={handleChange}
                    >
                        <MenuItem value="FULL_RECORD">FULL_RECORD</MenuItem>
                        <MenuItem value="METADATA_ONLY">METADATA_ONLY</MenuItem>
                    </Select>
                </FormControl>

                <TextField
                    label="Partition Regex"
                    name="partition_regex"
                    value={formData.partition_regex}
                    onChange={handleChange}
                    fullWidth
                    margin="normal"
                />

                <FormControlLabel
                    control={<Checkbox checked={formData.schema_validation} onChange={handleChange} name="schema_validation" />}
                    label="Schema Validation"
                />

                <FormControlLabel
                    control={<Checkbox checked={formData.dry_run} onChange={handleChange} name="dry_run" />}
                    label="Dry Run"
                />

                <Box sx={{ textAlign: 'center', mt: 2 }}>
                    <Button variant="contained" color="primary" type="submit" disabled={loading}>
                        {loading ? <CircularProgress size={24} /> : 'Bootstrap Hudi Table'}
                    </Button>
                </Box>

                {message.text && (
                    <Alert severity={message.severity} sx={{ mt: 2 }}>
                        {message.text}
                    </Alert>
                )}
            </form>
        </Box>
	<HistoryTable setFormData={setFormData} setLog={setLog} handleOpenLog={handleOpenLog} />

            {/* Display the Transaction Log Modal */}
        <TransactionLogModal open={logModalOpen} onClose={handleCloseLog} log={log} />
	</>
    );
}

export default App;

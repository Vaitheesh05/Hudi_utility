import React, { useState, useEffect } from 'react';
import {
    TextField,
    Button,
    Box,
    Typography,
    Alert,
    CircularProgress,
    Paper,
    Grid,
    MenuItem,
} from '@mui/material';
import axios from 'axios';
import { useLocation } from 'react-router-dom';

const defaultFormData = {
    data_file_path: "",
    hudi_table_name: "",
    key_field: "",
    precombine_field: "",
    partition_field: "",
    hudi_table_type: "COPY_ON_WRITE",
    write_operation: "insert",
    output_path: "",
    spark_config: JSON.stringify({ 'spark.executor.memory': '2g' }, null, 2),
    schema_validation: false,
    dry_run: false,
    bootstrap_type: "FULL_RECORD",
    partition_regex: ""
};

const BootstrapPage = ({ reset }) => {
    const location = useLocation();
    const [formData, setFormData] = useState(defaultFormData);
    const [loading, setLoading] = useState(false);
    const [message, setMessage] = useState({ text: '', severity: '' });

    useEffect(() => {
        if (location.state && location.state.formData) {
            setFormData(location.state.formData);
        } else {
            setFormData(defaultFormData); // Reset to default if no state is provided
        }
    }, [location.state]);

    useEffect(() => {
        if (reset) {
            setFormData(defaultFormData); // Reset to default on reset prop change
        }
    }, [reset]);

    const handleChange = (e) => {
        const { name, value, type, checked } = e.target;
        setFormData((prev) => ({
            ...prev,
            [name]: type === 'checkbox' ? checked : value
        }));
    };

    const handleSubmit = async (e) => {
	    e.preventDefault();
	    setLoading(true);
	    try {
		let sparkConfig = {};
		try {
		    sparkConfig = formData.spark_config ? JSON.parse(formData.spark_config) : {};
		} catch (jsonError) {
		    setMessage({ text: "Invalid Spark configuration JSON.", severity: 'error' });
		    setLoading(false);
		    return;
		}
		
		const requestData = {
		    ...formData,
		    spark_config: sparkConfig
		};
		
		const response = await axios.post("http://127.0.0.1:8000/bootstrap_hudi/", requestData);
		if (response.status === 200) {
		    setMessage({ text: response.data.message, severity: 'success' });
		}
	    } catch (error) {
		const errorMessage = error.response?.data?.detail || "Failed to bootstrap the Hudi table.";
		setMessage({ text: errorMessage, severity: 'error' });
	    }
	    setLoading(false);
	};


    return (
        <Box sx={{ display: 'flex', justifyContent: 'space-between', gap: 2, padding: 2 }}>
            {/* Left Section */}
            <Box sx={{ flex: 1 }}>
                {/* Top Card */}
                <Paper sx={{ padding: 2, marginBottom: 2, boxShadow: '0 4px 20px rgba(0, 0, 0, 0.1)' }}>
                    <Typography variant="h6" gutterBottom>
                        Data Source & Destination
                    </Typography>
                    <TextField
                        label="Data File Path"
                        name="data_file_path"
                        value={formData.data_file_path}
                        onChange={handleChange}
                        fullWidth
                        required
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                    <TextField
                        label="Output Path"
                        name="output_path"
                        value={formData.output_path}
                        onChange={handleChange}
                        fullWidth
                        required
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                </Paper>

                {/* Bottom Card */}
                <Paper sx={{ padding: 2, boxShadow: '0 4px 20px rgba(0, 0, 0, 0.1)' }}>
                    <Typography variant="h6" gutterBottom>
                        Table Configuration
                    </Typography>
                    <TextField
                        label="Hudi Table Name"
                        name="hudi_table_name"
                        value={formData.hudi_table_name}
                        onChange={handleChange}
                        fullWidth
                        required
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                    <TextField
                        label="Operation Type"
                        name="write_operation"
                        value={formData.write_operation}
                        onChange={handleChange}
                        fullWidth
                        select
                        SelectProps={{ native: false }}
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    >
                        <MenuItem value="insert">Insert</MenuItem>
                        <MenuItem value="upsert">Upsert</MenuItem>
                    </TextField>

                    {/* Grid for Bootstrap Type and Table Type */}
                    <Grid container spacing={2}>
                        <Grid item xs={6}>
                            <TextField
                                label="Table Type"
                                name="hudi_table_type"
                                value={formData.hudi_table_type}
                                onChange={handleChange}
                                fullWidth
                                select
                                SelectProps={{ native: false }}
                                variant="outlined"
                                sx={{ marginBottom: 2 }}
                            >
                                <MenuItem value="COPY_ON_WRITE">COPY_ON_WRITE</MenuItem>
                                <MenuItem value="MERGE_ON_READ">MERGE_ON_READ</MenuItem>
                            </TextField>
                        </Grid>
                        <Grid item xs={6}>
                            <TextField
                                label="Bootstrap Type"
                                name="bootstrap_type"
                                value={formData.bootstrap_type}
                                onChange={handleChange}
                                fullWidth
                                select
                                SelectProps={{ native: false }}
                                variant="outlined"
                                sx={{ marginBottom: 2 }}
                            >
                                <MenuItem value="FULL_RECORD">FULL_RECORD</MenuItem>
                                <MenuItem value="METADATA_ONLY">METADATA_ONLY</MenuItem>
                            </TextField>
                        </Grid>
                    </Grid>

                    <TextField
                        label="Partition Regex"
                        name="partition_regex"
                        value={formData.partition_regex}
                        onChange={handleChange}
                        fullWidth
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                </Paper>
            </Box>

            {/* Right Section */}
            <Box sx={{ flex: 1 }}>
                <Paper sx={{ padding: 2, height: '100%', boxShadow: '0 4px 20px rgba(0, 0, 0, 0.1)' }}>
                    <Typography variant="h6" gutterBottom>
                        Key Fields & Spark Configuration
                    </Typography>
                    <TextField
                        label="Key Field"
                        name="key_field"
                        value={formData.key_field}
                        onChange={handleChange}
                        fullWidth
                        required
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                    <TextField
                        label="Precombine Field"
                        name="precombine_field"
                        value={formData.precombine_field}
                        onChange={handleChange}
                        fullWidth
                        required
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                    <TextField
                        label="Partition Field"
                        name="partition_field"
                        value={formData.partition_field}
                        onChange={handleChange}
                        fullWidth
                        required
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                    <TextField
                        label="Spark Config (JSON)"
                        name="spark_config"
                        value={formData.spark_config}
                        onChange={handleChange}
                        fullWidth
                        multiline
                        rows={4}
                        variant="outlined"
                        sx={{ marginBottom: 2 }}
                    />
                    <Box sx={{ textAlign: 'center', marginTop: 2 }}>
                        <Button variant="contained" color="primary" type="submit" onClick={handleSubmit} disabled={loading}>
                            {loading ? <CircularProgress size={24} /> : 'Submit'}
                        </Button>
                    </Box>
                    {message.text && <Alert severity={message.severity} sx={{ marginTop: 2 }}>{message.text}</Alert>}
                </Paper>
            </Box>
        </Box>
    );
};

export default BootstrapPage;

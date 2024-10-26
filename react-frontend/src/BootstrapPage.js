// BootstrapPage.js
import React, { useState, useEffect } from 'react';
import {
    TextField,
    Button,
    Checkbox,
    FormControlLabel,
    Box,
    Typography,
    Alert,
    CircularProgress,
    Paper,
} from '@mui/material';
import axios from 'axios';
import { useLocation } from 'react-router-dom';

const BootstrapPage = () => {
    const location = useLocation();
    const [formData, setFormData] = useState({
        data_file_path: "",
        hudi_table_name: "",
        key_field: "",
        precombine_field: "",
        partition_field: "",
        hudi_table_type: "COPY_ON_WRITE",
        write_operation: "insert",
        output_path: "",
        spark_config: { 'spark.executor.memory': '2g' },
        schema_validation: false,
        dry_run: false,
        bootstrap_type: "FULL_RECORD",
        partition_regex: ""
    });

    const [loading, setLoading] = useState(false);
    const [message, setMessage] = useState({ text: '', severity: '' });

    useEffect(() => {
        if (location.state && location.state.formData) {
            setFormData(location.state.formData);
        }
    }, [location.state]);

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
            const response = await axios.post("http://127.0.0.1:8000/bootstrap_hudi/", formData);
            if (response.status === 200) {
                setMessage({ text: response.data.message, severity: 'success' });
            }
        } catch (error) {
            setMessage({ text: "Failed to bootstrap the Hudi table.", severity: 'error' });
        }
        setLoading(false);
    };

    return (
        <Paper elevation={3} sx={{ maxWidth: 600, margin: 'auto', padding: 3 }}>
            <Typography variant="h5" gutterBottom>
                New Bootstrapping Request
            </Typography>
            <form onSubmit={handleSubmit}>
                <TextField label="Data File Path" name="data_file_path" value={formData.data_file_path} onChange={handleChange} fullWidth required variant="outlined" sx={{ marginBottom: 2 }} />
                <TextField label="Hudi Table Name" name="hudi_table_name" value={formData.hudi_table_name} onChange={handleChange} fullWidth required variant="outlined" sx={{ marginBottom: 2 }} />
                <TextField label="Key Field" name="key_field" value={formData.key_field} onChange={handleChange} fullWidth required variant="outlined" sx={{ marginBottom: 2 }} />
                <TextField label="Precombine Field" name="precombine_field" value={formData.precombine_field} onChange ={handleChange} fullWidth required variant="outlined" sx={{ marginBottom: 2 }} />
                <TextField label="Partition Field" name="partition_field" value={formData.partition_field} onChange={handleChange} fullWidth required variant="outlined" sx={{ marginBottom: 2 }} />
                <TextField label="Output Path" name="output_path" value={formData.output_path} onChange={handleChange} fullWidth required variant="outlined" sx={{ marginBottom: 2 }} />
                <TextField label="Partition Regex" name="partition_regex" value={formData.partition_regex} onChange={handleChange} fullWidth variant="outlined" sx={{ marginBottom: 2 }} />
                <FormControlLabel control={<Checkbox checked={formData.schema_validation} onChange={handleChange} name="schema_validation" />} label="Schema Validation" />
                <FormControlLabel control={<Checkbox checked={formData.dry_run} onChange={handleChange} name="dry_run" />} label="Dry Run" />
                <Box sx={{ textAlign: 'center', marginTop: 2 }}>
                    <Button variant="contained" color="primary" type="submit" disabled={loading}>
                        {loading ? <CircularProgress size={24} /> : 'Submit'}
                    </Button>
                </Box>
                {message.text && <Alert severity={message.severity} sx={{ marginTop: 2 }}>{message.text}</Alert>}
            </form>
        </Paper>
    );
};

export default BootstrapPage;

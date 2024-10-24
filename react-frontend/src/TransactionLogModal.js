import React from 'react';
import { Dialog, DialogTitle, DialogContent, IconButton, Typography } from '@mui/material';
import CloseIcon from '@mui/icons-material/Close';

function TransactionLogModal({ open, onClose, log }) {
    return (
        <Dialog open={open} onClose={onClose} maxWidth="md" fullWidth>
            <DialogTitle>
                Transaction Log
                <IconButton
                    aria-label="close"
                    onClick={onClose}
                    style={{ position: 'absolute', right: 8, top: 8 }}
                >
                    <CloseIcon />
                </IconButton>
            </DialogTitle>
            <DialogContent dividers style={{ maxHeight: '400px', overflowY: 'auto' }}>
                <Typography variant="body1" style={{ whiteSpace: 'pre-wrap' }}>
                    {log || 'No log available for this transaction.'}
                </Typography>
            </DialogContent>
        </Dialog>
    );
}

export default TransactionLogModal;


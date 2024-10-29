// theme.js
import { createTheme } from '@mui/material/styles';
import { red } from '@mui/material/colors'; // Import colors if needed

const theme = createTheme({
    palette: {
        mode: 'light', // or 'dark' based on your preference
        primary: {
            main: '#212636', // Example primary color
        },
        secondary: {
            main: '#635bff', // Example secondary color
        },
        background: {
            default: '#f4f4f4', // Default background color
            paper: '#ffffff', // Paper background color
        },
        action: {
            focus: '#212636', // Focus border color
            hover: '#212636', // Hover color (optional)
            selected: '#212636', // Selected color (optional)
        },
    },
    typography: {
        fontFamily: '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
    },
    components: {
        MuiAppBar: {
            styleOverrides: {
                root: {
                    backgroundColor: '#212636', // AppBar background color
                },
            },
        },
        MuiButton: {
            styleOverrides: {
                root: {
                    borderRadius: '12px', // Custom border radius
                    textTransform: 'none', // Disable uppercase text
                    '&:focus': {
                        outline: '1px solid #212636', // Focus outline
                    },
                    '&:selected': {
                        outline: '1px solid #212636', // Selected outline
                    },
                },
            },
        },
        MuiCard: {
            styleOverrides: {
                root: {
                    borderRadius: '20px',
                },
            },
        },
        MuiTableHead: {
            styleOverrides: {
                root: {
                    backgroundColor: 'var(--mui-palette-background-level1)',
                    color: 'var(--mui-palette-text-secondary)',
                },
            },
        },
        MuiTableCell: {
            styleOverrides: {
                root: {
                    borderBottom: '1px solid var(--mui-palette-TableCell-border)',
                },
            },
        },
        MuiTableBody: {
            styleOverrides: {
                root: {
                    '& .MuiTableRow:last-child td': {
                        borderBottom: 0,
                    },
                },
            },
        },
        MuiTextField: {
            styleOverrides: {
                root: {
		    fontFamily: '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
                    '& .MuiOutlinedInput-root': {
                        '&:hover fieldset': {
                            borderColor: '#212636', // Border color on hover
			    borderWidth: '1px',
                        },
                        '&.Mui-focused fieldset': {
                            borderColor: '#212636', // Border color when focused
			    borderWidth: '1px',
                        },
                    },
                },
            },
        },
	MuiMenuItem: {
            styleOverrides: {
                root: {
                    fontFamily: '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif', // Apply font family to MenuItem
                },
            },
        },
        MuiDatePicker: {
            styleOverrides: {
                root: {
		    fontFamily: '"Inter", -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif',
                    '& .MuiOutlinedInput-root': {
                        '& fieldset': {
                            borderColor: '#212636', // Default border color
			    borderWidth: '1px',
                        },
                        '&:hover fieldset': {
                            borderColor: '#10bee8', // Border color on hover
			    borderWidth: '1px',
                        },
                        '&.Mui-focused fieldset': {
                            borderColor: '#212636', // Border color when focused
			    borderWidth: '1px',
                        },
                    },
                },
            },
        },
    },
});

export default theme;


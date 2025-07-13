import ChevronLeftIcon from "@mui/icons-material/ChevronLeft";
import ChevronRightIcon from "@mui/icons-material/ChevronRight";
import Box from "@mui/material/Box";
import IconButton from "@mui/material/IconButton";
import Typography from "@mui/material/Typography";
import SelectMenu from "../../SelectMenu";

export interface IDataGridPaginationProps {
  limit?: number;
  page?: number;
  total?: number;
  results?: number;
  onChangeLimit: (newLimit: number) => void;
  onChangePage: (newPage: number) => void;
}

export default function DataGridPagination({
  limit,
  page,
  total,
  results,
  onChangeLimit,
  onChangePage,
}: IDataGridPaginationProps) {
  const nextPage = () => {
    if (page === undefined) {
      return;
    }

    const updated = page + 1;
    onChangePage(updated);
  };

  const prevPage = () => {
    if (page === undefined) return;
    if (page === 1) return;

    const updated = page - 1;
    onChangePage(updated);
  };

  const handleChangeLimit = (value: string) => {
    const newLimit = Number(value);
    onChangeLimit(newLimit);
  };

  return (
    <Box
      sx={{
        display: "flex",
        alignItems: "center",
        justifyContent: "flex-end",
        gap: 2,
        paddingX: 2,
        paddingY: 1,
      }}
    >
      <Box sx={{ display: "flex", alignItems: "center" }}>
        <Typography variant="body2" mr={1}>
          Items per page:
        </Typography>
        <SelectMenu
          label={limit ?? 10}
          options={[10, 25, 50, 100]}
          onSelect={handleChangeLimit}
        />
      </Box>
      <Box sx={{ display: "flex", alignItems: "center" }}>
        <Typography variant="body2">Page: {page}</Typography>
        <Box ml={1}>
          <IconButton onClick={prevPage}>
            <ChevronLeftIcon />
          </IconButton>
          <IconButton onClick={nextPage}>
            <ChevronRightIcon />
          </IconButton>
        </Box>
      </Box>
      <Box>
        <Typography variant="body2" mr={1}>
          Results: {results}
        </Typography>
      </Box>
      <Box>
        <Typography variant="body2" mr={1}>
          Total: {total}
        </Typography>
      </Box>
    </Box>
  );
}

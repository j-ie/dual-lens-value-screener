import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import CompanyDetailPage from "./CompanyDetailPage";
import ScreeningListPage from "./ScreeningListPage";

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<ScreeningListPage />} />
        <Route path="/runs/:runId/companies/:tsCode" element={<CompanyDetailPage />} />
        <Route path="*" element={<Navigate to="/" replace />} />
      </Routes>
    </BrowserRouter>
  );
}

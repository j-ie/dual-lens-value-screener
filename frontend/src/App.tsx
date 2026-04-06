import { BrowserRouter, Navigate, Route, Routes } from "react-router-dom";
import AiHistoryPage from "./AiHistoryPage";
import { AppShell } from "./components/AppShell";
import CompanyDetailPage from "./CompanyDetailPage";
import DataTasksPage from "./DataTasksPage";
import ScreeningListPage from "./ScreeningListPage";

export default function App() {
  return (
    <BrowserRouter>
      <AppShell>
        <Routes>
          <Route path="/" element={<ScreeningListPage />} />
          <Route path="/data-tasks" element={<DataTasksPage />} />
          <Route path="/ai-history" element={<AiHistoryPage />} />
          <Route path="/runs/:runId/companies/:tsCode" element={<CompanyDetailPage />} />
          <Route path="*" element={<Navigate to="/" replace />} />
        </Routes>
      </AppShell>
    </BrowserRouter>
  );
}

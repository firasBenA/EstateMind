export default function SigNozDashboard() {
  return (
    <div className="h-screen w-full">
      <iframe 
        src="http://localhost:3301" 
        className="w-full h-full border-0"
        title="SigNoz Monitoring"
      />
    </div>
  );
}
import { UserDashboardLayout } from "@/components/UserDashboardLayout";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { useState } from "react";

const timelineData = [
  { year: 0, label: "Today", investment: 320000, value: 320000, rent: 0 },
  { year: 1, label: "Year 1", investment: 320000, value: 329600, rent: 9600 },
  { year: 3, label: "Year 3", investment: 320000, value: 352000, rent: 28800 },
  { year: 5, label: "Year 5", investment: 320000, value: 380000, rent: 48000 },
  { year: 10, label: "Year 10", investment: 320000, value: 460000, rent: 108000 },
  { year: 15, label: "Year 15", investment: 320000, value: 550000, rent: 180000 },
  { year: 20, label: "Year 20", investment: 320000, value: 680000, rent: 264000 },
];

export default function ProfitabilityPlanner() {
  const [activeYear, setActiveYear] = useState(10);
  const entry = timelineData.find(t => t.year === activeYear) || timelineData[4];
  const totalReturn = entry.value + entry.rent - entry.investment;
  const roi = ((totalReturn / entry.investment) * 100).toFixed(1);

  return (
    <UserDashboardLayout>
      <div className="max-w-4xl mx-auto space-y-8">
        <div className="text-center space-y-2">
          <h1 className="text-3xl font-bold">Profitability Planner</h1>
          <p className="text-muted-foreground">See how your investment grows over time</p>
        </div>

        {/* Animated Timeline */}
        <div className="relative">
          <div className="flex justify-between items-center py-8">
            {timelineData.map((point, i) => {
              const isActive = point.year === activeYear;
              const isPast = point.year <= activeYear;
              return (
                <button
                  key={point.year}
                  onClick={() => setActiveYear(point.year)}
                  className="relative flex flex-col items-center group z-10"
                >
                  <div
                    className={`h-5 w-5 rounded-full border-2 transition-all duration-500 ${
                      isActive ? "bg-primary border-primary scale-150 shadow-lg shadow-primary/30" :
                      isPast ? "bg-primary/60 border-primary/60" : "bg-muted border-border"
                    }`}
                  />
                  <span className={`text-xs mt-2 transition-all duration-300 ${isActive ? "font-bold text-primary scale-110" : "text-muted-foreground"}`}>
                    {point.label}
                  </span>
                </button>
              );
            })}
            {/* Line */}
            <div className="absolute top-[2.55rem] left-0 right-0 h-0.5 bg-border" />
            <div
              className="absolute top-[2.55rem] left-0 h-0.5 bg-primary transition-all duration-700 ease-out"
              style={{ width: `${(timelineData.findIndex(t => t.year === activeYear) / (timelineData.length - 1)) * 100}%` }}
            />
          </div>
        </div>

        {/* Results */}
        <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
          <Card className="border-2 border-primary/20">
            <CardContent className="pt-6 text-center">
              <p className="text-sm text-muted-foreground mb-1">Property Value</p>
              <p className="text-3xl font-bold text-primary transition-all duration-500">{entry.value.toLocaleString()} TND</p>
              <Badge variant="secondary" className="mt-2">+{((entry.value / entry.investment - 1) * 100).toFixed(0)}% appreciation</Badge>
            </CardContent>
          </Card>
          <Card>
            <CardContent className="pt-6 text-center">
              <p className="text-sm text-muted-foreground mb-1">Rental Income</p>
              <p className="text-3xl font-bold transition-all duration-500">{entry.rent.toLocaleString()} TND</p>
              <p className="text-xs text-muted-foreground mt-2">Cumulative over {entry.year} years</p>
            </CardContent>
          </Card>
          <Card className="bg-success/5 border-success/20">
            <CardContent className="pt-6 text-center">
              <p className="text-sm text-muted-foreground mb-1">Total ROI</p>
              <p className="text-3xl font-bold text-success transition-all duration-500">+{roi}%</p>
              <p className="text-xs text-muted-foreground mt-2">Net return: {totalReturn.toLocaleString()} TND</p>
            </CardContent>
          </Card>
        </div>

        {/* Visual bar chart */}
        <Card>
          <CardHeader><CardTitle className="text-base">Investment Growth Breakdown</CardTitle></CardHeader>
          <CardContent>
            <div className="space-y-4">
              {timelineData.filter(t => t.year > 0).map(point => {
                const total = point.value + point.rent;
                const maxVal = timelineData[timelineData.length - 1].value + timelineData[timelineData.length - 1].rent;
                const width = (total / maxVal) * 100;
                const propWidth = (point.value / maxVal) * 100;
                const rentWidth = (point.rent / maxVal) * 100;
                const isActive = point.year === activeYear;
                return (
                  <div key={point.year} className={`transition-all duration-300 ${isActive ? "scale-[1.02]" : ""}`}>
                    <div className="flex justify-between text-xs mb-1">
                      <span className={isActive ? "font-bold text-primary" : "text-muted-foreground"}>{point.label}</span>
                      <span className="text-muted-foreground">{total.toLocaleString()} TND</span>
                    </div>
                    <div className="h-6 rounded-full bg-muted overflow-hidden flex">
                      <div
                        className="h-full bg-primary transition-all duration-700 ease-out"
                        style={{ width: `${propWidth}%` }}
                      />
                      <div
                        className="h-full bg-chart-2 transition-all duration-700 ease-out"
                        style={{ width: `${rentWidth}%` }}
                      />
                    </div>
                  </div>
                );
              })}
              <div className="flex gap-4 text-xs pt-2">
                <div className="flex items-center gap-2"><div className="h-3 w-3 rounded bg-primary" /> Property Value</div>
                <div className="flex items-center gap-2"><div className="h-3 w-3 rounded bg-chart-2" /> Rental Income</div>
              </div>
            </div>
          </CardContent>
        </Card>

        <div className="text-center">
          <p className="text-sm text-muted-foreground mb-3">Want a detailed analysis? Generate a full investment report.</p>
          <Button asChild><a href="/user/reports">Generate Full Report</a></Button>
        </div>
      </div>
    </UserDashboardLayout>
  );
}

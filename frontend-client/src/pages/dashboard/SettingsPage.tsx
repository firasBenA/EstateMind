
import { DashboardLayout } from "@/components/DashboardLayout";
import { useAuth } from "@/lib/auth-context";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { toast } from "sonner";
import { useState } from "react";

export default function SettingsPage() {
  const { user } = useAuth();
  const [newPassword, setNewPassword] = useState("");
  const [inviteEmail, setInviteEmail] = useState("");
  const [inviteRole, setInviteRole] = useState("analyst");

  const teamMembers = [
    { email: "admin@estatemind.tn", role: "admin" },
    { email: "analyst@estatemind.tn", role: "analyst" },
  ];

  return (
    <DashboardLayout>
      <div className="max-w-3xl space-y-6">
        {/* Profile */}
        <Card>
          <CardHeader><CardTitle className="text-lg">Profile</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            <div className="flex items-center gap-3">
              <div className="w-12 h-12 rounded-full bg-primary/10 flex items-center justify-center text-primary font-bold text-lg">
                {user?.email?.charAt(0).toUpperCase()}
              </div>
              <div>
                <div className="font-medium">{user?.email}</div>
                <Badge variant="secondary" className="capitalize">{user?.role}</Badge>
              </div>
            </div>
          </CardContent>
        </Card>

        {/* Change Password */}
        <Card>
          <CardHeader><CardTitle className="text-lg">Change Password</CardTitle></CardHeader>
          <CardContent>
            <div className="flex gap-3 items-end">
              <div className="flex-1 space-y-2">
                <Label>New Password</Label>
                <Input type="password" value={newPassword} onChange={e => setNewPassword(e.target.value)} placeholder="Enter new password" />
              </div>
              <Button onClick={() => { setNewPassword(""); toast.success("Password updated"); }}>Update</Button>
            </div>
          </CardContent>
        </Card>

        {/* Team */}
        <Card>
          <CardHeader><CardTitle className="text-lg">Team Members</CardTitle></CardHeader>
          <CardContent>
            <div className="space-y-3 mb-6">
              {teamMembers.map(m => (
                <div key={m.email} className="flex items-center justify-between border rounded-lg p-3">
                  <span className="text-sm">{m.email}</span>
                  <Badge variant="secondary" className="capitalize">{m.role}</Badge>
                </div>
              ))}
            </div>
            <div className="space-y-3">
              <h4 className="font-medium text-sm">Invite Member</h4>
              <div className="flex gap-3 items-end">
                <div className="flex-1 space-y-2">
                  <Label>Email</Label>
                  <Input type="email" value={inviteEmail} onChange={e => setInviteEmail(e.target.value)} placeholder="user@example.com" />
                </div>
                <div className="w-32 space-y-2">
                  <Label>Role</Label>
                  <Select value={inviteRole} onValueChange={setInviteRole}>
                    <SelectTrigger><SelectValue /></SelectTrigger>
                    <SelectContent>
                      <SelectItem value="admin">Admin</SelectItem>
                      <SelectItem value="analyst">Analyst</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Button onClick={() => { setInviteEmail(""); toast.success("Invite sent"); }}>Send Invite</Button>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>
    </DashboardLayout>
  );
}

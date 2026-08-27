import { FarmDetailClient } from "./FarmDetailClient";

export default async function FarmDetailPage({ params }: { params: Promise<{ id: string }> }) {
  const { id } = await params;
  return <FarmDetailClient id={id} />;
}

export type Provider = "groq" | "gemini";

export const PROVIDER_IDS: Provider[] = ["groq", "gemini"];

const PROVIDER_NAMES: Record<Provider, string> = {
  groq: "Groq",
  gemini: "Google AI Studio",
};

/** Nombre fijo del proveedor (sin el modelo, que depende del env — ver lib/coach.ts). */
export function providerName(id: Provider): string {
  return PROVIDER_NAMES[id];
}

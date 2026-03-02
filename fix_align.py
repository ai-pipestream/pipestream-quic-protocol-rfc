import re

with open("sections-src/section-06.md", "r") as f:
    lines = f.read().split("\n")

new_lines = []
in_artwork = False

for line in lines:
    if line == '~~~~':
        in_artwork = not in_artwork
        new_lines.append(line)
        continue
    
    if not in_artwork:
        new_lines.append(line)
        continue
    
    # Inside artwork
    if line.strip().startswith("+-"):
        new_lines.append("   +-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+")
    elif line.strip().startswith("0 1 2"):
        new_lines.append("    0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1")
    elif line.strip() == "0                   1                   2                   3":
        new_lines.append("    0                   1                   2                   3")
    elif line.strip().startswith("|"):
        # Make sure the line starts with "   |"
        # Calculate the text inside
        inner = line.strip()[1:-1].strip()
        
        # specific hardcoded cases for exact centerings
        if "Entity ID (32 bits)" in inner and "Parent" not in inner:
            new_lines.append("   |                       Entity ID (32 bits)                     |")
        elif "Scope ID (16" in inner and "Reserved (16" in inner:
            new_lines.append("   |        Scope ID (16 bits)       |      Reserved (16 bits)     |")
        elif "New Cursor Value" in inner:
            new_lines.append("   |                  New Cursor Value (32 bits)                   |")
        elif "Type (0x54)" in inner:
            new_lines.append("   |  Type (0x54)  |  Flags (8)      |        Scope ID (16)        |")
        elif "Entities Processed" in inner:
            new_lines.append("   |                   Entities Processed (64 bits)                |")
        elif "Entities Succeeded" in inner:
            new_lines.append("   |                   Entities Succeeded (64 bits)                |")
        elif "Entities Failed" in inner:
            new_lines.append("   |                    Entities Failed (64 bits)                  |")
        elif "Entities Deferred" in inner:
            new_lines.append("   |                    Entities Deferred (64 bits)                |")
        elif "Merkle Root" in inner:
            new_lines.append("   |                    Merkle Root (256 bits)                     |")
        elif "Type (0x55)" in inner:
            new_lines.append("   |  Type (0x55)  |S|  Reserved (7) |        Barrier ID (16)      |")
        elif "Parent Entity ID" in inner:
            new_lines.append("   |                    Parent Entity ID (32 bits)                 |")
        elif "Type (0x50)" in inner:
            new_lines.append("   |  Type (0x50)  | Stat(4)|E|C|D|       Flags (15 bits)         |")
        elif "Yield Reason" in inner:
            new_lines.append("   | Yield Reason  |           Token Length (24 bits)              |")
        elif "Yield Token (variable)" in inner:
            new_lines.append("   |                  Yield Token (variable)                       |")
        elif "Claim Check ID" in inner:
            new_lines.append("   |                    Claim Check ID (64 bits)                   |")
        elif "Expiry Timestamp" in inner:
            new_lines.append("   |                Expiry Timestamp (64 bits, Unix micros)        |")
        elif line.strip() == "|                                                               |":
            new_lines.append("   |                                                               |")
        elif "Header Length" in inner:
            new_lines.append("   |    Header Length (4)      |   4 octets, big-endian uint32")
        elif "Header (Protobuf)" in inner:
            new_lines.append("   |    Header (Protobuf)      |   Variable length")
        elif "Payload" in inner:
            new_lines.append("   |    Payload                |   Variable length (per header)")
        else:
            new_lines.append(line)
    else:
        new_lines.append(line)

with open("sections-src/section-06.md", "w") as f:
    f.write("\n".join(new_lines))
